package kubernetes

import (
	"strings"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/netobserv/flowlogs-pipeline/pkg/operational"
	"github.com/netobserv/flowlogs-pipeline/pkg/pipeline/transform/kubernetes/cni"
	inf "github.com/netobserv/flowlogs-pipeline/pkg/pipeline/transform/kubernetes/informers"
	"github.com/sirupsen/logrus"
)

var informers inf.InformersInterface = &inf.Informers{}

// For testing
func MockInformers() {
	informers = inf.NewInformersMock()
}

func InitFromConfig(config api.NetworkTransformKubeConfig, opMetrics *operational.Metrics) error {
	return informers.InitFromConfig(config, opMetrics)
}

func Enrich(outputEntry config.GenericMap, rule *api.K8sRule) {
	ip, ok := outputEntry.LookupString(rule.IPField)
	if !ok {
		return
	}
	ips := []string{ip}
	var additionalIPs []string
	potentialKeys := informers.BuildSecondaryNetworkKeys(outputEntry, rule)
	if moreIPsAny, ok := outputEntry["Additional"+rule.IPField]; ok {
		if additionalIPs, ok = moreIPsAny.([]string); ok {
			ips = append(ips, additionalIPs...)
		}
	}
	ipInfos := getMultipleIPsKubeInfo(potentialKeys, ips)
	kubeInfo := ipInfos.main
	if kubeInfo == nil {
		logrus.Tracef("can't find kubernetes info for keys %v and IPs %v", potentialKeys, ips)
		return
	}
	if ipInfos.mainIP != ip {
		// Promote main IP
		outputEntry[rule.IPField] = ipInfos.mainIP
		if len(ipInfos.otherIPs) > 0 {
			outputEntry["Additional"+rule.IPField] = ipInfos.otherIPs
		}
		// TODO: promote port as well
	}
	if rule.Assignee != "otel" {
		// NETOBSERV-666: avoid putting empty namespaces or Loki aggregation queries will
		// differentiate between empty and nil namespaces.
		if kubeInfo.Namespace != "" {
			outputEntry[rule.Output+"_Namespace"] = kubeInfo.Namespace
		}
		outputEntry[rule.Output+"_Name"] = kubeInfo.Name
		outputEntry[rule.Output+"_Type"] = kubeInfo.Type
		outputEntry[rule.Output+"_OwnerName"] = kubeInfo.Owner.Name
		outputEntry[rule.Output+"_OwnerType"] = kubeInfo.Owner.Type
		outputEntry[rule.Output+"_NetworkName"] = kubeInfo.NetworkName
		if ipInfos.serviceIP != "" {
			outputEntry[rule.Output+"_ServiceIP"] = ipInfos.serviceIP
			outputEntry[rule.Output+"_ServiceName"] = ipInfos.serviceName
		}
		if rule.LabelsPrefix != "" {
			for labelKey, labelValue := range kubeInfo.Labels {
				outputEntry[rule.LabelsPrefix+"_"+labelKey] = labelValue
			}
		}
		if kubeInfo.HostIP != "" {
			outputEntry[rule.Output+"_HostIP"] = kubeInfo.HostIP
			if kubeInfo.HostName != "" {
				outputEntry[rule.Output+"_HostName"] = kubeInfo.HostName
			}
		}
		fillInK8sZone(outputEntry, rule, kubeInfo, "_Zone")
	} else {
		// NOTE: Some of these fields are taken from opentelemetry specs.
		// See https://opentelemetry.io/docs/specs/semconv/resource/k8s/
		// Other fields (not specified in the specs) are named similarly
		if kubeInfo.Namespace != "" {
			outputEntry[rule.Output+"k8s.namespace.name"] = kubeInfo.Namespace
		}
		switch kubeInfo.Type {
		case inf.TypeNode:
			outputEntry[rule.Output+"k8s.node.name"] = kubeInfo.Name
			outputEntry[rule.Output+"k8s.node.uid"] = kubeInfo.UID
		case inf.TypePod:
			outputEntry[rule.Output+"k8s.pod.name"] = kubeInfo.Name
			outputEntry[rule.Output+"k8s.pod.uid"] = kubeInfo.UID
			if ipInfos.serviceIP != "" {
				outputEntry[rule.Output+"k8s.service.ip"] = ipInfos.serviceIP
				outputEntry[rule.Output+"k8s.service.name"] = ipInfos.serviceName
			}
		case inf.TypeService:
			outputEntry[rule.Output+"k8s.service.name"] = kubeInfo.Name
			outputEntry[rule.Output+"k8s.service.uid"] = kubeInfo.UID
		}
		outputEntry[rule.Output+"k8s.name"] = kubeInfo.Name
		outputEntry[rule.Output+"k8s.type"] = kubeInfo.Type
		outputEntry[rule.Output+"k8s.owner.name"] = kubeInfo.Owner.Name
		outputEntry[rule.Output+"k8s.owner.type"] = kubeInfo.Owner.Type
		if rule.LabelsPrefix != "" {
			for labelKey, labelValue := range kubeInfo.Labels {
				outputEntry[rule.LabelsPrefix+"."+labelKey] = labelValue
			}
		}
		if kubeInfo.HostIP != "" {
			outputEntry[rule.Output+"k8s.host.ip"] = kubeInfo.HostIP
			if kubeInfo.HostName != "" {
				outputEntry[rule.Output+"k8s.host.name"] = kubeInfo.HostName
			}
		}
		fillInK8sZone(outputEntry, rule, kubeInfo, "k8s.zone")
	}
}

type MultiIPsInfo struct {
	main        *inf.Info
	mainIP      string
	serviceName string
	serviceIP   string
	otherIPs    []string
}

func getMultipleIPsKubeInfo(potentialKeys []cni.SecondaryNetKey, ips []string) *MultiIPsInfo {
	var podInfo, serviceInfo, otherInfo *inf.Info
	var podIP, serviceIP, otherIP string
	var otherIPs []string
	for _, ip := range ips {
		kubeInfo, _ := informers.GetInfo(potentialKeys, ip)
		if kubeInfo != nil {
			switch kubeInfo.Type {
			case "Pod":
				if podInfo == nil {
					podInfo = kubeInfo
					podIP = ip
				} else {
					otherIPs = append(otherIPs, ip)
				}
			case "Service":
				if serviceInfo == nil {
					serviceInfo = kubeInfo
					serviceIP = ip
				} else {
					otherIPs = append(otherIPs, ip)
				}
			default:
				if otherInfo == nil {
					otherInfo = kubeInfo
					otherIP = ip
				} else {
					otherIPs = append(otherIPs, ip)
				}
			}
		} else {
			otherIPs = append(otherIPs, ip)
		}
	}
	if otherIP != "" && (podInfo != nil || serviceInfo != nil) {
		// Do not loose track of otherIP
		otherIPs = append(otherIPs, otherIP)
	}
	if podInfo != nil {
		if serviceInfo != nil {
			return &MultiIPsInfo{
				main:        podInfo,
				mainIP:      podIP,
				serviceName: serviceInfo.Name,
				serviceIP:   serviceIP,
				otherIPs:    otherIPs,
			}
		}
		return &MultiIPsInfo{
			main:     podInfo,
			mainIP:   podIP,
			otherIPs: otherIPs,
		}
	}
	if serviceInfo != nil {
		return &MultiIPsInfo{
			main:     serviceInfo,
			mainIP:   serviceIP,
			otherIPs: otherIPs,
		}
	}
	return &MultiIPsInfo{
		main:     otherInfo,
		mainIP:   otherIP,
		otherIPs: otherIPs,
	}
}

const nodeZoneLabelName = "topology.kubernetes.io/zone"

func fillInK8sZone(outputEntry config.GenericMap, rule *api.K8sRule, kubeInfo *inf.Info, zonePrefix string) {
	if !rule.AddZone {
		// Nothing to do
		return
	}
	switch kubeInfo.Type {
	case inf.TypeNode:
		zone, ok := kubeInfo.Labels[nodeZoneLabelName]
		if ok {
			outputEntry[rule.Output+zonePrefix] = zone
		}
		return
	case inf.TypePod:
		nodeInfo, err := informers.GetNodeInfo(kubeInfo.HostName)
		if err != nil {
			logrus.WithError(err).Tracef("can't find nodes info for node %v", kubeInfo.HostName)
			return
		}
		if nodeInfo != nil {
			zone, ok := nodeInfo.Labels[nodeZoneLabelName]
			if ok {
				outputEntry[rule.Output+zonePrefix] = zone
			}
		}
		return

	case inf.TypeService:
		// A service is not assigned to a dedicated zone, skipping
		return
	}
}

func EnrichLayer(outputEntry config.GenericMap, rule *api.K8sInfraRule) {
	outputEntry[rule.Output] = "infra"
	for _, nsnameFields := range rule.NamespaceNameFields {
		if namespace, _ := outputEntry.LookupString(nsnameFields.Namespace); namespace != "" {
			name, _ := outputEntry.LookupString(nsnameFields.Name)
			if objectIsApp(namespace, name, rule) {
				outputEntry[rule.Output] = "app"
				return
			}
		}
	}
}

func objectIsApp(namespace, name string, rule *api.K8sInfraRule) bool {
	for _, prefix := range rule.InfraPrefixes {
		if strings.HasPrefix(namespace, prefix) {
			return false
		}
	}
	for _, ref := range rule.InfraRefs {
		if namespace == ref.Namespace && name == ref.Name {
			return false
		}
	}
	return true
}
