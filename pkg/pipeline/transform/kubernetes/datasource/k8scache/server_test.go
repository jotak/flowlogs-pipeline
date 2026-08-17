package k8scache

import (
	"context"
	"fmt"
	"io"
	"testing"

	pb "github.com/netobserv/flowlogs-pipeline/pkg/pipeline/transform/kubernetes/k8scache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestKubernetesCacheServer_ReceivesAdd tests that when a client sends an ADD,
// the server processes it correctly
func TestKubernetesCacheServer_ReceivesAdd(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	// Create mock stream
	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// Client (informers) sends ADD update
	addUpdate := &pb.CacheUpdate{
		Version:    1,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "test-ns-1",
				Name:      "test-pod-1",
				Uid:       "pod-uid-1",
				Ips:       []string{"10.0.0.1"},
			},
		},
	}
	mockStream.sendChan <- addUpdate
	close(mockStream.sendChan)

	// Run server
	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Verify server sent SyncRequest first
	require.Greater(t, len(mockStream.recvMsgs), 0, "Should have sent at least SyncRequest")
	firstMsg := mockStream.recvMsgs[0]
	req, ok := firstMsg.Message.(*pb.SyncMessage_Request)
	require.True(t, ok, "First message should be SyncRequest")
	assert.NotEmpty(t, req.Request.ProcessorId)

	// Verify server sent ACK
	require.Greater(t, len(mockStream.recvMsgs), 1, "Should have sent ACK")
	ackMsg := mockStream.recvMsgs[1]
	ack, ok := ackMsg.Message.(*pb.SyncMessage_Ack)
	require.True(t, ok, "Second message should be ACK")
	assert.True(t, ack.Ack.Success)
	assert.Equal(t, int64(1), ack.Ack.Version)

	// Verify resource was added to store
	meta := server.IndexLookup(nil, "10.0.0.1")
	require.NotNil(t, meta, "Resource should be in store")
	assert.Equal(t, "test-pod-1", meta.Name)
}

// TestKubernetesCacheServer_ReceivesIncrementalUpdate tests incremental updates
func TestKubernetesCacheServer_ReceivesIncrementalUpdate(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// Send incremental update
	update := &pb.CacheUpdate{
		Version:    2,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "default",
				Name:      "new-pod",
			},
		},
	}
	mockStream.sendChan <- update
	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Verify ACK was sent
	require.Greater(t, len(mockStream.recvMsgs), 1)
	ackMsg := mockStream.recvMsgs[1]
	ack, ok := ackMsg.Message.(*pb.SyncMessage_Ack)
	require.True(t, ok)
	assert.True(t, ack.Ack.Success)
	assert.Equal(t, int64(2), ack.Ack.Version)
}

// TestKubernetesCacheServer_MultipleUpdates tests receiving multiple updates in sequence
func TestKubernetesCacheServer_MultipleUpdates(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// Send ADD
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    1,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries:    []*pb.ResourceEntry{{Kind: "Pod", Name: "pod1", Namespace: "default"}},
	}

	// Send another ADD
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    2,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries:    []*pb.ResourceEntry{{Kind: "Pod", Name: "pod2", Namespace: "default"}},
	}

	// Send DELETE
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    3,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_DELETE,
		Entries:    []*pb.ResourceEntry{{Kind: "Pod", Name: "pod1", Namespace: "default"}},
	}

	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Should have: 1 SyncRequest + 3 ACKs
	assert.Equal(t, 4, len(mockStream.recvMsgs))

	// Verify version tracking
	assert.Equal(t, int64(3), server.GetCurrentVersion())
}

// TestKubernetesCacheServer_ErrorHandling tests error scenarios
func TestKubernetesCacheServer_ErrorHandling(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	t.Run("client disconnects abruptly", func(t *testing.T) {
		mockStream := &mockStreamServer{
			ctx:       context.Background(),
			sendChan:  make(chan *pb.CacheUpdate, 10),
			recvMsgs:  make([]*pb.SyncMessage, 0),
			firstSend: true,
		}

		// Close immediately
		close(mockStream.sendChan)

		// Should handle gracefully
		err := server.StreamUpdates(mockStream)
		assert.NoError(t, err) // EOF is normal when client disconnects
	})
}

// TestKubernetesCacheServer_WithKubernetesStore tests operations using KubernetesStore
func TestKubernetesCacheServer_WithKubernetesStore(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// Send ADD
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    1,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "test-ns",
				Name:      "store-pod-1",
				Uid:       "store-pod-uid-1",
				Ips:       []string{"10.0.1.1"},
				Labels: map[string]string{
					"app": "test",
				},
			},
		},
	}

	// Send UPDATE
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    2,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_UPDATE,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "test-ns",
				Name:      "store-pod-1",
				Uid:       "store-pod-uid-1",
				Ips:       []string{"10.0.1.1"},
				Labels: map[string]string{
					"app":     "test",
					"version": "v2", // Updated label
				},
			},
		},
	}

	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Verify resource was added and updated in store
	meta := server.IndexLookup(nil, "10.0.1.1")
	require.NotNil(t, meta, "Resource should be in KubernetesStore")
	assert.Equal(t, "store-pod-1", meta.Name)
	assert.Equal(t, "test-ns", meta.Namespace)
	assert.Equal(t, "v2", meta.Labels["version"], "Label should be updated")

	// Verify ACKs were sent
	require.Equal(t, 3, len(mockStream.recvMsgs)) // SyncRequest + 2 ACKs
}

// TestKubernetesCacheServer_DeleteFromStore tests DELETE operation on KubernetesStore
func TestKubernetesCacheServer_DeleteFromStore(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// First ADD a resource
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    1,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "test-ns",
				Name:      "delete-me",
				Uid:       "delete-pod-uid",
				Ips:       []string{"10.0.2.1"},
			},
		},
	}

	// Then DELETE it
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    2,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_DELETE,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "test-ns",
				Name:      "delete-me",
			},
		},
	}

	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Verify resource was deleted from store
	meta := server.IndexLookup(nil, "10.0.2.1")
	assert.Nil(t, meta, "Resource should be deleted from KubernetesStore")
}

// TestKubernetesCacheServer_StoreReplacesInformers tests that KubernetesStore replaces Informers
func TestKubernetesCacheServer_StoreReplacesInformers(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// Add resource via gRPC (goes to KubernetesStore)
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    1,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "grpc-ns",
				Name:      "grpc-pod",
				Ips:       []string{"10.0.3.1"},
			},
		},
	}

	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Should find resource from KubernetesStore
	grpcMeta := server.IndexLookup(nil, "10.0.3.1")
	require.NotNil(t, grpcMeta)
	assert.Equal(t, "grpc-pod", grpcMeta.Name)

	// When KubernetesStore is set, Informers are bypassed
	// (testIPInfo from setupTestDatasourceWithStore is in Informers, not in Store)
	informerMeta := server.IndexLookup(nil, "10.0.0.1")
	assert.Nil(t, informerMeta, "KubernetesStore replaces Informers, so Informer data is not accessible")
}

// TestKubernetesCacheServer_SnapshotReplace tests that the server properly handles
// a full snapshot (is_snapshot=true) by calling Replace() instead of AddOrUpdate()
func TestKubernetesCacheServer_SnapshotReplace(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// First, send a snapshot with initial data
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    1,
		IsSnapshot: true, // This is a full snapshot
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "test-ns",
				Name:      "snapshot-pod-1",
				Uid:       "snapshot-pod-uid-1",
				Ips:       []string{"10.0.10.1"},
			},
			{
				Kind:      "Pod",
				Namespace: "test-ns",
				Name:      "snapshot-pod-2",
				Uid:       "snapshot-pod-uid-2",
				Ips:       []string{"10.0.10.2"},
			},
		},
	}

	// Then send another snapshot that should replace the entire store
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    2,
		IsSnapshot: true, // This is a full snapshot
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "test-ns",
				Name:      "snapshot-pod-3",
				Uid:       "snapshot-pod-uid-3",
				Ips:       []string{"10.0.10.3"},
			},
		},
	}

	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Verify first snapshot was replaced by second snapshot
	// The first two pods should NOT be in the store
	meta1 := server.IndexLookup(nil, "10.0.10.1")
	assert.Nil(t, meta1, "First snapshot pod should be replaced")

	meta2 := server.IndexLookup(nil, "10.0.10.2")
	assert.Nil(t, meta2, "First snapshot pod should be replaced")

	// Only the third pod from the second snapshot should exist
	meta3 := server.IndexLookup(nil, "10.0.10.3")
	require.NotNil(t, meta3, "Second snapshot pod should exist")
	assert.Equal(t, "snapshot-pod-3", meta3.Name)

	// Verify ACKs were sent for both snapshots
	require.Equal(t, 3, len(mockStream.recvMsgs)) // SyncRequest + 2 ACKs
}

// TestKubernetesCacheServer_MultiBatchSnapshot verifies that a snapshot split across
// multiple batches (first batch Replace, rest ADD) retains all entries in the store.
func TestKubernetesCacheServer_MultiBatchSnapshot(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	const batchSize = 100
	const totalEntries = 150

	firstBatch := make([]*pb.ResourceEntry, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		firstBatch = append(firstBatch, &pb.ResourceEntry{
			Kind:      "Pod",
			Namespace: "test-ns",
			Name:      fmt.Sprintf("pod-%d", i),
			Uid:       fmt.Sprintf("uid-%d", i),
			Ips:       []string{fmt.Sprintf("10.1.%d.1", i)},
		})
	}
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    1,
		IsSnapshot: true,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries:    firstBatch,
	}

	secondBatch := make([]*pb.ResourceEntry, 0, totalEntries-batchSize)
	for i := batchSize; i < totalEntries; i++ {
		secondBatch = append(secondBatch, &pb.ResourceEntry{
			Kind:      "Pod",
			Namespace: "test-ns",
			Name:      fmt.Sprintf("pod-%d", i),
			Uid:       fmt.Sprintf("uid-%d", i),
			Ips:       []string{fmt.Sprintf("10.1.%d.1", i)},
		})
	}
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    2,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries:    secondBatch,
	}

	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	for i := 0; i < totalEntries; i++ {
		meta := server.IndexLookup(nil, fmt.Sprintf("10.1.%d.1", i))
		require.NotNil(t, meta, "entry %d should be present after multi-batch snapshot", i)
	}
}

// TestKubernetesCacheServer_InitialSyncWithSnapshot tests the typical scenario where
// a fresh processor (LastVersion=0) receives a full snapshot from the client
func TestKubernetesCacheServer_InitialSyncWithSnapshot(t *testing.T) {
	server := NewKubernetesCacheDatasource()

	mockStream := &mockStreamServer{
		ctx:       context.Background(),
		sendChan:  make(chan *pb.CacheUpdate, 10),
		recvMsgs:  make([]*pb.SyncMessage, 0),
		firstSend: true,
	}

	// Server should send SyncRequest with LastVersion=0
	// Client responds with a full snapshot
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    100, // Some arbitrary version number from the client
		IsSnapshot: true,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "kube-system",
				Name:      "coredns-1",
				Ips:       []string{"10.96.0.10"},
			},
			{
				Kind:      "Node",
				Namespace: "",
				Name:      "worker-1",
				Ips:       []string{"192.168.1.10"},
			},
		},
	}

	// After the snapshot, incremental updates follow
	mockStream.sendChan <- &pb.CacheUpdate{
		Version:    101,
		IsSnapshot: false,
		Operation:  pb.OperationType_OPERATION_ADD,
		Entries: []*pb.ResourceEntry{
			{
				Kind:      "Pod",
				Namespace: "kube-system",
				Name:      "coredns-2",
				Ips:       []string{"10.96.0.11"},
			},
		},
	}

	close(mockStream.sendChan)

	err := server.StreamUpdates(mockStream)
	require.NoError(t, err)

	// Verify SyncRequest was sent with LastVersion=0
	require.Greater(t, len(mockStream.recvMsgs), 0)
	firstMsg := mockStream.recvMsgs[0]
	req, ok := firstMsg.Message.(*pb.SyncMessage_Request)
	require.True(t, ok)
	assert.Equal(t, int64(0), req.Request.LastVersion, "Initial sync should request from version 0")

	// Verify both pods are in the store (snapshot + incremental)
	pod1 := server.IndexLookup(nil, "10.96.0.10")
	require.NotNil(t, pod1)
	assert.Equal(t, "coredns-1", pod1.Name)

	pod2 := server.IndexLookup(nil, "10.96.0.11")
	require.NotNil(t, pod2)
	assert.Equal(t, "coredns-2", pod2.Name)

	// Verify node is in the store
	node, err := server.GetNodeByName("worker-1")
	require.NoError(t, err)
	require.NotNil(t, node)
	assert.Equal(t, "worker-1", node.Name)

	// Verify version was updated to the latest
	assert.Equal(t, int64(101), server.GetCurrentVersion())
}

// mockStreamServer implements the server-side stream for testing
// Note: With the corrected protocol, the server:
// - Receives CacheUpdate (from client)
// - Sends SyncMessage (to client)
type mockStreamServer struct {
	grpc.ServerStream
	ctx       context.Context
	sendChan  chan *pb.CacheUpdate // What client sends
	recvMsgs  []*pb.SyncMessage    // What server sent
	firstSend bool
}

func (m *mockStreamServer) Context() context.Context {
	return m.ctx
}

// Send is called by the server to send SyncMessage to client
func (m *mockStreamServer) Send(msg *pb.SyncMessage) error {
	m.recvMsgs = append(m.recvMsgs, msg)
	return nil
}

// Recv is called by the server to receive CacheUpdate from client
func (m *mockStreamServer) Recv() (*pb.CacheUpdate, error) {
	update, ok := <-m.sendChan
	if !ok {
		return nil, io.EOF
	}
	return update, nil
}
