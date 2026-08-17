package k8scache

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	pb "github.com/netobserv/flowlogs-pipeline/pkg/pipeline/transform/kubernetes/k8scache"
	"github.com/netobserv/flowlogs-pipeline/pkg/pipeline/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var slog = log.WithField("component", "k8scache.Server")

// KubernetesCacheDatasource implements the gRPC KubernetesCacheService server.
// It receives cache updates from the centralized informers client.
type KubernetesCacheDatasource struct {
	*KubernetesStore
	pb.UnimplementedKubernetesCacheServiceServer
	grpcServer *grpc.Server
	version    atomic.Int64 // Last version received
}

// NewKubernetesCacheDatasource creates a new cache synchronization server/datasource.
func NewKubernetesCacheDatasource() *KubernetesCacheDatasource {
	return &KubernetesCacheDatasource{
		KubernetesStore: NewKubernetesStore(),
	}
}

func (s *KubernetesCacheDatasource) Stop() {
	if s.grpcServer != nil {
		slog.Info("stopping K8s cache sync server")
		// GracefulStop can hang indefinitely if StreamUpdates connections are still active
		// Use a timeout and fall back to force-stop if needed
		stopped := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			slog.Info("K8s cache sync server stopped gracefully")
		case <-time.After(5 * time.Second):
			slog.Warn("timed out waiting for K8s cache sync streams to drain; forcing stop")
			s.grpcServer.Stop()
		}
	}
}

// StreamUpdates implements the bidirectional streaming RPC
// The server:
//  1. Sends SyncRequest to ask for data
//  2. Receives CacheUpdate from client
//  3. Sends SyncAck to confirm receipt
//  4. Repeat steps 2-3
func (s *KubernetesCacheDatasource) StreamUpdates(stream pb.KubernetesCacheService_StreamUpdatesServer) error {
	ctx := stream.Context()

	// Generate a unique ID for this connection (for logging)
	connectionID := fmt.Sprintf("flp-%d", time.Now().UnixNano())

	// Resync on Reconnect: Always request a full snapshot when establishing a new stream.
	// This ensures consistency after any kind of informer-leader failover, network partition,
	// or other disconnection scenarios, regardless of the cause.
	// The cost is minimal (snapshot sent only on reconnection, not during normal operation).
	slog.WithField("connection_id", connectionID).Info("New stream connection - resetting version to request full snapshot")
	s.version.Store(0)

	// Send SyncRequest with LastVersion=0 to request full snapshot
	lastVersion := s.version.Load() // Will always be 0
	err := stream.Send(&pb.SyncMessage{
		Message: &pb.SyncMessage_Request{
			Request: &pb.SyncRequest{
				ProcessorId: connectionID,
				LastVersion: lastVersion,
			},
		},
	})
	if err != nil {
		slog.WithError(err).Error("Failed to send initial SyncRequest")
		return err
	}

	slog.WithFields(log.Fields{
		"connection_id": connectionID,
		"last_version":  lastVersion,
	}).Info("Sent SyncRequest to client")

	// Receive updates from client
	for {
		select {
		case <-ctx.Done():
			slog.WithField("connection_id", connectionID).Info("Connection context cancelled")
			return ctx.Err()
		default:
			update, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				slog.WithField("connection_id", connectionID).Info("Client disconnected gracefully")
				return nil
			}
			if err != nil {
				slog.WithError(err).WithField("connection_id", connectionID).Warn("Error receiving from client")
				return err
			}

			// Process the update
			if err := s.processUpdate(update); err != nil {
				slog.WithError(err).WithField("version", update.Version).Error("Failed to process update")
				// Send NACK
				_ = stream.Send(&pb.SyncMessage{
					Message: &pb.SyncMessage_Ack{
						Ack: &pb.SyncAck{
							ProcessorId: connectionID,
							Version:     update.Version,
							Success:     false,
							Error:       err.Error(),
						},
					},
				})
				continue
			}

			// Update was processed successfully
			s.version.Store(update.Version)

			// Send ACK
			err = stream.Send(&pb.SyncMessage{
				Message: &pb.SyncMessage_Ack{
					Ack: &pb.SyncAck{
						ProcessorId: connectionID,
						Version:     update.Version,
						Success:     true,
					},
				},
			})
			if err != nil {
				slog.WithError(err).Error("Failed to send ACK")
				return err
			}

			slog.WithFields(log.Fields{
				"connection_id": connectionID,
				"version":       update.Version,
				"is_snapshot":   update.IsSnapshot,
				"num_entries":   len(update.Entries),
			}).Debug("Processed and acknowledged update")
		}
	}
}

// processUpdate applies a cache update to the local datasource (when KubernetesStore is set).
func (s *KubernetesCacheDatasource) processUpdate(update *pb.CacheUpdate) error {
	entries := pb.ResourceEntriesToMeta(update.Entries)

	// Handle full snapshot (when client sends is_snapshot=true, typically when LastVersion=0)
	if update.IsSnapshot {
		slog.WithField("num_entries", len(entries)).Info("Received full snapshot, replacing store")
		s.Replace(entries)
		return nil
	}

	// Handle incremental updates
	switch update.Operation {
	case pb.OperationType_OPERATION_ADD, pb.OperationType_OPERATION_UPDATE:
		slog.WithField("num_entries", len(entries)).Debug("Received ADD/UPDATE")
		s.AddOrUpdate(entries)
	case pb.OperationType_OPERATION_DELETE:
		slog.WithField("num_entries", len(entries)).Debug("Received DELETE")
		s.Delete(entries)
	case pb.OperationType_OPERATION_UNSPECIFIED:
		return fmt.Errorf("received update with unspecified operation")
	default:
		return fmt.Errorf("unknown operation type: %v", update.Operation)
	}

	return nil
}

// GetCurrentVersion returns the last version received
func (s *KubernetesCacheDatasource) GetCurrentVersion() int64 {
	return s.version.Load()
}

// StartGRPC initializes and starts the gRPC server for K8s cache synchronization
func (s *KubernetesCacheDatasource) StartGRPC(cfg *api.K8sCacheServer) {
	// Configure keepalive and resource limits to prevent resource exhaustion
	// These settings protect against misbehaving clients and ensure graceful connection management
	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second, // Minimum time between client pings
		PermitWithoutStream: false,           // Require active stream for keepalive
	}
	kaParams := keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Minute, // Close idle connections
		MaxConnectionAge:      30 * time.Minute, // Max connection lifetime
		MaxConnectionAgeGrace: 5 * time.Second,  // Grace period before forcing close
		Time:                  30 * time.Second, // Ping interval when idle
		Timeout:               10 * time.Second, // Ping timeout
	}

	// Base server options (applied to both TLS and non-TLS)
	serverOpts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(100),            // Limit concurrent streams per connection
		grpc.KeepaliveParams(kaParams),            // Configure keepalive behavior
		grpc.KeepaliveEnforcementPolicy(kaPolicy), // Enforce keepalive policy
		grpc.MaxRecvMsgSize(50 * 1024 * 1024),     // 50MB max message size
	}

	// Create gRPC server with optional TLS
	if cfg.TLSEnabled {
		tlsConfig, err := createServerTLSConfig(cfg)
		if err != nil {
			slog.WithError(err).Fatal("failed to configure TLS for K8s cache server")
		}
		s.grpcServer = grpc.NewServer(append(serverOpts, grpc.Creds(tlsConfig))...)
		slog.Info("K8s cache server TLS enabled")
	} else {
		s.grpcServer = grpc.NewServer(serverOpts...)
		slog.Warn("K8s cache server TLS disabled - connections are insecure (not recommended for production)")
	}
	pb.RegisterKubernetesCacheServiceServer(s.grpcServer, s)

	// Start listening
	// Use net.JoinHostPort to properly handle IPv6 addresses (adds brackets when needed)
	address := net.JoinHostPort(cfg.Address, fmt.Sprintf("%d", cfg.Port))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		slog.WithError(err).WithField("address", address).Fatal("failed to start K8s cache server")
	}

	// Start server in background
	go func() {
		slog.WithField("address", address).Info("starting K8s cache sync server")
		if err := s.grpcServer.Serve(listener); err != nil {
			slog.WithError(err).Error("K8s cache sync server stopped with error")
		}
	}()

	go func() {
		<-utils.ExitChannel()
		s.Stop()
	}()
}

// createServerTLSConfig creates TLS credentials for the gRPC server
func createServerTLSConfig(cfg *api.K8sCacheServer) (credentials.TransportCredentials, error) {
	// Load server certificate and private key
	if cfg.TLSCertPath == "" || cfg.TLSKeyPath == "" {
		return nil, fmt.Errorf("TLS enabled but cert/key paths not provided")
	}

	cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert/key: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert, // Default: no client cert required
		MinVersion:   tls.VersionTLS13, // Enforce TLS 1.3+ to prevent downgrade attacks
	}

	// If CA is provided, require and verify client certificates
	if cfg.TLSCAPath != "" {
		caCert, err := os.ReadFile(cfg.TLSCAPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA cert")
		}

		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		slog.Info("K8s cache server: mutual TLS enabled (client certificates required)")
	} else {
		slog.Warn("K8s cache server: TLS enabled but no client certificate verification (no CA provided). " +
			"Any client with TLS can connect. Use --k8scache.tls-ca-path for mTLS or ensure network policies restrict access.")
	}

	return credentials.NewTLS(tlsConfig), nil
}
