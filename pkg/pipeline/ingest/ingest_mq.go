package ingest

import (
	"time"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/netobserv/flowlogs-pipeline/pkg/operational"
	"github.com/netobserv/flowlogs-pipeline/pkg/pipeline/decode"
	"github.com/netobserv/flowlogs-pipeline/pkg/pipeline/utils"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

var mqlog = logrus.WithField("component", "ingest.MQ")

type ingestMQ struct {
	decoder  decode.Decoder
	in       <-chan amqp.Delivery
	exitChan <-chan struct{}
	metrics  *metrics
}

const queueName = "flows"

// Ingest ingests entries from kafka topic
func (m *ingestMQ) Ingest(out chan<- config.GenericMap) {
	mqlog.Debugf("entering ingestMQ.Ingest")
	m.metrics.createOutQueueLen(out)

	//	go func() {
	for msg := range m.in {
		if m.isStopped() {
			mqlog.Info("gracefully exiting")
			return
		}
		mqlog.Trace("fetching messages from queue")
		m.metrics.flowsProcessed.Inc()
		messageLen := len(msg.Body)
		m.metrics.batchSizeBytes.Observe(float64(messageLen))
		if messageLen > 0 {
			m.processRecord(msg.Body, out)
		}
	}
	//	}()

	// forever process log lines received by collector
	//	m.processLogLines(out)
}

func (m *ingestMQ) isStopped() bool {
	select {
	case <-m.exitChan:
		return true
	default:
		return false
	}
}

func (m *ingestMQ) processRecordDelay(record config.GenericMap) {
	timeFlowEndInterface, ok := record["TimeFlowEndMs"]
	if !ok {
		// "trace" level used to minimize performance impact
		mqlog.Tracef("TimeFlowEndMs missing in record %v", record)
		m.metrics.error("TimeFlowEndMs missing")
		return
	}
	timeFlowEnd, ok := timeFlowEndInterface.(int64)
	if !ok {
		// "trace" level used to minimize performance impact
		mqlog.Tracef("Cannot parse TimeFlowEndMs of record %v", record)
		m.metrics.error("Cannot parse TimeFlowEndMs")
		return
	}
	delay := time.Since(time.UnixMilli(timeFlowEnd)).Seconds()
	m.metrics.latency.Observe(delay)
}

func (m *ingestMQ) processRecord(record []byte, out chan<- config.GenericMap) {
	// Decode batch
	decoded, err := m.decoder.Decode(record)
	if err != nil {
		mqlog.WithError(err).Warnf("ignoring flow")
		return
	}
	m.processRecordDelay(decoded)

	// Send batch
	out <- decoded
}

func NewIngestMQ(opMetrics *operational.Metrics, params config.StageParam) (Ingester, error) {
	config := &api.IngestKafka{}
	var ingestType string
	if params.Ingest != nil {
		ingestType = params.Ingest.Type
		if params.Ingest.Kafka != nil {
			config = params.Ingest.Kafka
		}
	}

	conn, err := amqp.Dial(config.Brokers[0])
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	decoder, err := decode.GetDecoder(config.Decoder)
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	in, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return nil, err
	}

	metrics := newMetrics(opMetrics, params.Name, ingestType, func() int { return len(in) })

	return &ingestMQ{
		decoder:  decoder,
		exitChan: utils.ExitChannel(),
		in:       in,
		metrics:  metrics,
	}, nil
}
