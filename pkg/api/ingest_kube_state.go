package api

type IngestKubeState struct {
	ConfigPath string                `yaml:"configPath,omitempty" json:"configPath,omitempty" doc:"path to kubeconfig file (optional)"`
	Kinds      []IngestKubeStateKind `yaml:"kinds,omitempty" json:"kinds,omitempty" doc:"list of kinds to watch"`
}

type IngestKubeStateKind struct {
	Group     string                 `yaml:"group,omitempty" json:"group,omitempty" doc:"API group"`
	Version   string                 `yaml:"version,omitempty" json:"version,omitempty" doc:"API version"`
	Resource  string                 `yaml:"resource,omitempty" json:"resource,omitempty" doc:"API resource name"`
	Namespace string                 `yaml:"namespace,omitempty" json:"namespace,omitempty" doc:"Namespace to watch; leave empty for cluster-wide watch"`
	Fields    []IngestKubeStateField `yaml:"fields,omitempty" json:"fields,omitempty" doc:"List of fields to extract"`
}

type IngestKubeStateField struct {
	Name     string `yaml:"name,omitempty" json:"name,omitempty" doc:"Output field name"`
	JSONPath string `yaml:"jsonPath,omitempty" json:"jsonPath,omitempty" doc:"JSON-path to the field to extract"`
	Type     string `yaml:"type,omitempty" json:"type,omitempty" doc:"Type of data to extract: String or Number (??)"`
}
