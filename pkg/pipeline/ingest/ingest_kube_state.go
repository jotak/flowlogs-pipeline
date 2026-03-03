package ingest

import (
	"bytes"
	"fmt"
	"time"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/netobserv/flowlogs-pipeline/pkg/operational"
	putils "github.com/netobserv/flowlogs-pipeline/pkg/pipeline/utils"
	"github.com/netobserv/flowlogs-pipeline/pkg/utils"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/jsonpath"
)

const (
	syncTime = 10 * time.Minute
)

var iklog = logrus.WithField("component", "ingest.KubeState")

type ingestKubeState struct {
	kinds      []api.IngestKubeStateKind
	kubeClient *dynamic.DynamicClient
	exitChan   <-chan struct{}
	metrics    *metrics
}

func NewIngestKubeState(opMetrics *operational.Metrics, params config.StageParam) (Ingester, error) {
	config := &api.IngestKubeState{}
	var ingestType string
	if params.Ingest != nil {
		ingestType = params.Ingest.Type
		if params.Ingest.KubeState != nil {
			config = params.Ingest.KubeState
		}
	}

	kconf, err := utils.LoadK8sConfig(config.ConfigPath)
	if err != nil {
		return nil, err
	}

	kubeClient, err := dynamic.NewForConfig(kconf)
	if err != nil {
		return nil, err
	}

	metrics := newMetrics(
		opMetrics,
		params.Name,
		ingestType,
		func() int { return 0 },
	)

	return &ingestKubeState{
		kinds:      config.Kinds,
		kubeClient: kubeClient,
		exitChan:   putils.ExitChannel(),
		metrics:    metrics,
	}, nil
}

func (k *ingestKubeState) Ingest(out chan<- config.GenericMap) {
	k.metrics.createOutQueueLen(out)

	// Initialize informers
	// TODO: optimization: if all fields are in meta, use meta informer
	perNamespaceFactory := make(map[string]dynamicinformer.DynamicSharedInformerFactory)
	for i := range k.kinds {
		kind := &k.kinds[i]
		factory := perNamespaceFactory[kind.Namespace]
		if factory == nil {
			factory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(k.kubeClient, syncTime, kind.Namespace, nil)
			perNamespaceFactory[kind.Namespace] = factory
		}
		if err := k.createInformer(out, factory, kind, i); err != nil {
			iklog.Error(err)
			k.metrics.error("Cannot create informer")
		}
	}

	iklog.Debug("Starting Kubernetes informers")
	for ns := range perNamespaceFactory {
		perNamespaceFactory[ns].Start(k.exitChan)
	}
	for ns := range perNamespaceFactory {
		perNamespaceFactory[ns].WaitForCacheSync(k.exitChan)
	}
	iklog.Debug("Kubernetes informers started")
	<-k.exitChan
	iklog.Debug("Gracefully exiting Kube State ingester")
}

func (k *ingestKubeState) createInformer(out chan<- config.GenericMap, informerFactory dynamicinformer.DynamicSharedInformerFactory, cfg *api.IngestKubeStateKind, index int) error {
	type resourceMetaData struct {
		// Informers need that internal object is an ObjectMeta instance
		metav1.ObjectMeta
		transformed config.GenericMap
	}
	gvr := schema.GroupVersionResource{
		Group:    cfg.Group,
		Version:  cfg.Version,
		Resource: cfg.Resource,
	}
	gvrStr := gvr.String()
	informer := informerFactory.ForResource(gvr).Informer()
	if err := informer.SetTransform(func(i any) (any, error) {
		obj, ok := i.(*unstructured.Unstructured)
		if !ok {
			k.metrics.error("Conversion error")
			return nil, fmt.Errorf("[gvr=%s], informer: was expecting a *unstructured.Unstructured, got: %T", gvrStr, i)
		}
		transformed := make(config.GenericMap, 3+len(cfg.Fields))
		transformed["_index"] = index
		transformed["_gvr"] = gvrStr
		if cfg.Namespace != "" {
			transformed["_namespace"] = cfg.Namespace
		}
		for _, f := range cfg.Fields {
			jp := jsonpath.New(f.Name)
			// TODO: precompute
			if err := jp.Parse(f.JSONPath); err != nil {
				iklog.Errorf("[gvr=%s, field=%s], bad jsonpath: %v", gvrStr, f.Name, err)
			}
			buf := new(bytes.Buffer)
			if err := jp.Execute(buf, obj.Object); err != nil {
				iklog.Errorf("[gvr=%s, field=%s], jsonpath error: %v", gvrStr, f.Name, err)
			}
			transformed[f.Name] = buf.String()
		}
		iklog.Debugf("Called transform hook [gvr=%s, result=%v]", gvrStr, transformed)
		return &resourceMetaData{
			ObjectMeta: metav1.ObjectMeta{
				Name:      obj.GetName(),
				Namespace: obj.GetNamespace(),
			},
			transformed: transformed,
		}, nil
	}); err != nil {
		return fmt.Errorf("[gvr=%s], informer: can't set transform: %w", gvrStr, err)
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if r, ok := obj.(*resourceMetaData); ok {
				k.metrics.flowsProcessed.Inc()
				iklog.Debugf("Event handler: Add [gvr=%s, result=%v]", gvrStr, r.transformed)
				out <- r.transformed
			} else {
				k.metrics.error("Conversion error")
				iklog.Errorf("[gvr=%s], informer: was expecting a GenericMap, got: %T", gvrStr, obj)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if r, ok := newObj.(*resourceMetaData); ok {
				k.metrics.flowsProcessed.Inc()
				iklog.Debugf("Event handler: Update [gvr=%s, result=%v]", gvrStr, r.transformed)
				out <- r.transformed
			} else {
				k.metrics.error("Conversion error")
				iklog.Errorf("[gvr=%s], informer: was expecting a GenericMap, got: %T", gvrStr, newObj)
			}
		},
	}); err != nil {
		return fmt.Errorf("[gvr=%s], informer: can't add event handler: %w", gvrStr, err)
	}
	return nil
}
