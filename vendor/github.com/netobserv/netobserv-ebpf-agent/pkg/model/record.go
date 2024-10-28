package model

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/netobserv/netobserv-ebpf-agent/pkg/ebpf"
)

// Values according to field 61 in https://www.iana.org/assignments/ipfix/ipfix.xhtml
const (
	DirectionIngress = uint8(0)
	DirectionEgress  = uint8(1)
)
const MacLen = 6

// IPv4Type / IPv6Type value as defined in IEEE 802: https://www.iana.org/assignments/ieee-802-numbers/ieee-802-numbers.xhtml
const (
	IPv6Type                 = 0x86DD
	networkEventsMaxEventsMD = 8
	maxNetworkEvents         = 4
)

type HumanBytes uint64
type MacAddr [MacLen]uint8
type Direction uint8

// IPAddr encodes v4 and v6 IPs with a fixed length.
// IPv4 addresses are encoded as IPv6 addresses with prefix ::ffff/96
// as described in https://datatracker.ietf.org/doc/html/rfc4038#section-4.2
// (same behavior as Go's net.IP type)
type IPAddr [net.IPv6len]uint8

// record structure as parsed from eBPF
type RawRecord ebpf.BpfFlowRecordT

// Record contains accumulated metrics from a flow
type Record struct {
	RawRecord
	// TODO: redundant field from RecordMetrics. Reorganize structs
	TimeFlowStart time.Time
	TimeFlowEnd   time.Time
	DNSLatency    time.Duration
	Interfaces    []string
	// AgentIP provides information about the source of the flow (the Agent that traced it)
	AgentIP net.IP
	// Calculated RTT which is set when record is created by calling NewRecord
	TimeFlowRtt            time.Duration
	NetworkMonitorEventsMD []string
}

func NewRecord(
	key ebpf.BpfFlowId,
	metrics *ebpf.BpfFlowMetrics,
	currentTime time.Time,
	monotonicCurrentTime uint64,
) *Record {
	startDelta := time.Duration(monotonicCurrentTime - metrics.StartMonoTimeTs)
	endDelta := time.Duration(monotonicCurrentTime - metrics.EndMonoTimeTs)

	var record = Record{
		RawRecord: RawRecord{
			Id:      key,
			Metrics: *metrics,
		},
		TimeFlowStart: currentTime.Add(-startDelta),
		TimeFlowEnd:   currentTime.Add(-endDelta),
	}
	if metrics.FlowRtt != 0 {
		record.TimeFlowRtt = time.Duration(metrics.FlowRtt)
	}
	if metrics.DnsRecord.Latency != 0 {
		record.DNSLatency = time.Duration(metrics.DnsRecord.Latency)
	}
	record.NetworkMonitorEventsMD = make([]string, 0)
	return &record
}

func Accumulate(r *ebpf.BpfFlowMetrics, src *ebpf.BpfFlowMetrics) {
	// time == 0 if the value has not been yet set
	if r.StartMonoTimeTs == 0 || (r.StartMonoTimeTs > src.StartMonoTimeTs && src.StartMonoTimeTs != 0) {
		r.StartMonoTimeTs = src.StartMonoTimeTs
	}
	if r.EndMonoTimeTs == 0 || r.EndMonoTimeTs < src.EndMonoTimeTs {
		r.EndMonoTimeTs = src.EndMonoTimeTs
	}
	r.Bytes += src.Bytes
	r.Packets += src.Packets
	r.Flags |= src.Flags
	if src.EthProtocol != 0 {
		r.EthProtocol = src.EthProtocol
	}
	// Accumulate interfaces + directions
	accumulateInterfaces(&r.NbObservedIntf, &r.ObservedIntf, src.NbObservedIntf, src.ObservedIntf)
	// Accumulate additional IPs
	accumulateIPs(&r.NbObservedSrcIps, &r.ObservedSrcIps, src.NbObservedSrcIps, src.ObservedSrcIps)
	accumulateIPs(&r.NbObservedDstIps, &r.ObservedDstIps, src.NbObservedDstIps, src.ObservedDstIps)
	if allZero(r.SrcMac) {
		r.SrcMac = src.SrcMac
	}
	if allZero(r.DstMac) {
		r.DstMac = src.DstMac
	}
	// Accumulate Drop statistics
	r.PktDrops.Bytes += src.PktDrops.Bytes
	r.PktDrops.Packets += src.PktDrops.Packets
	r.PktDrops.LatestFlags |= src.PktDrops.LatestFlags
	if src.PktDrops.LatestDropCause != 0 {
		r.PktDrops.LatestDropCause = src.PktDrops.LatestDropCause
	}
	// Accumulate DNS
	r.DnsRecord.Flags |= src.DnsRecord.Flags
	if src.DnsRecord.Id != 0 {
		r.DnsRecord.Id = src.DnsRecord.Id
	}
	if r.DnsRecord.Errno != src.DnsRecord.Errno {
		r.DnsRecord.Errno = src.DnsRecord.Errno
	}
	if r.DnsRecord.Latency < src.DnsRecord.Latency {
		r.DnsRecord.Latency = src.DnsRecord.Latency
	}
	// Accumulate RTT
	if r.FlowRtt < src.FlowRtt {
		r.FlowRtt = src.FlowRtt
	}
	// Accumulate DSCP
	if src.Dscp != 0 {
		r.Dscp = src.Dscp
	}

	for _, md := range src.NetworkEvents {
		if !AllZerosMetaData(md) && !networkEventsMDExist(r.NetworkEvents, md) {
			copy(r.NetworkEvents[r.NetworkEventsIdx][:], md[:])
			r.NetworkEventsIdx = (r.NetworkEventsIdx + 1) % maxNetworkEvents
		}
	}
}

func networkEventsMDExist(events [maxNetworkEvents][networkEventsMaxEventsMD]uint8, md [networkEventsMaxEventsMD]uint8) bool {
	for _, e := range events {
		// TODO: use uint8Equals, more performant?
		if reflect.DeepEqual(e, md) {
			return true
		}
	}
	return false
}

func accumulateInterfaces(dstSize *uint8, dstIntf *[4]ebpf.BpfPktObservationT, srcSize uint8, srcIntf [4]ebpf.BpfPktObservationT) {
	iObs := uint8(0)
outer:
	for *dstSize < uint8(len(dstIntf)) && iObs < srcSize {
		for u := uint8(0); u < *dstSize; u++ {
			if dstIntf[u].Direction == srcIntf[iObs].Direction &&
				dstIntf[u].IfIndex == srcIntf[iObs].IfIndex {
				// Ignore if already exists
				iObs++
				continue outer
			}
		}
		dstIntf[*dstSize] = srcIntf[iObs]
		*dstSize++
		iObs++
	}
}

func accumulateIPs(dstSize *uint8, dstIPs *[4][4]uint8, srcSize uint8, srcIPs [4][4]uint8) {
	iObs := uint8(0)
outer:
	for *dstSize < uint8(len(*dstIPs)) && iObs < srcSize {
		for u := uint8(0); u < *dstSize; u++ {
			if uint8Equals(dstIPs[u][:], srcIPs[iObs][:]) {
				// Ignore if already exists
				iObs++
				continue outer
			}
		}
		dstIPs[*dstSize] = srcIPs[iObs]
		*dstSize++
		iObs++
	}
}

func uint8Equals(a, b []uint8) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// IP returns the net.IP equivalent object
func IP(ia IPAddr) net.IP {
	return ia[:]
}

// IntEncodeV4 encodes an IPv4 address as an integer (in network encoding, big endian).
// It assumes that the passed IP is already IPv4. Otherwise it would just encode the
// last 4 bytes of an IPv6 address
func IntEncodeV4(ia [net.IPv6len]uint8) uint32 {
	return binary.BigEndian.Uint32(ia[net.IPv6len-net.IPv4len : net.IPv6len])
}

// IPAddrFromNetIP returns IPAddr from net.IP
func IPAddrFromNetIP(netIP net.IP) IPAddr {
	var arr [net.IPv6len]uint8
	copy(arr[:], (netIP)[0:net.IPv6len])
	return arr
}

func (ia *IPAddr) MarshalJSON() ([]byte, error) {
	return []byte(`"` + IP(*ia).String() + `"`), nil
}

func (m *MacAddr) String() string {
	return fmt.Sprintf("%02X:%02X:%02X:%02X:%02X:%02X", m[0], m[1], m[2], m[3], m[4], m[5])
}

func (m *MacAddr) MarshalJSON() ([]byte, error) {
	return []byte("\"" + m.String() + "\""), nil
}

// ReadFrom reads a Record from a binary source, in LittleEndian order
func ReadFrom(reader io.Reader) (*RawRecord, error) {
	var fr RawRecord
	err := binary.Read(reader, binary.LittleEndian, &fr)
	return &fr, err
}

func AllZerosMetaData(s [networkEventsMaxEventsMD]uint8) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func allZero(s [6]uint8) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}
