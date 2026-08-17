package datasource

import (
	"github.com/netobserv/flowlogs-pipeline/pkg/pipeline/transform/kubernetes/model"
)

type Datasource interface {
	IndexLookup(potentialKeys []string, ip string) *model.ResourceMetaData
	GetNodeByName(name string) (*model.ResourceMetaData, error)
}
