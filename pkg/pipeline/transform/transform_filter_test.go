/*
 * Copyright (C) 2022 IBM, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transform

import (
	"testing"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/netobserv/flowlogs-pipeline/pkg/test"
	"github.com/stretchr/testify/require"
)

const testConfigTransformFilterRemoveField = `---
log-level: debug
pipeline:
  - name: filter1
parameters:
  - name: filter1
    transform:
      type: filter
      filter:
        rules:
        - input: dstPort
          type: remove_field
        - input: srcPort
          type: remove_field
`

const testConfigTransformFilterRemoveEntryIfExists = `---
log-level: debug
pipeline:
  - name: filter1
parameters:
  - name: filter1
    transform:
      type: filter
      filter:
        rules:
        - input: srcPort
          type: remove_entry_if_exists
`

const testConfigTransformFilterRemoveEntryIfDoesntExists = `---
log-level: debug
pipeline:
  - name: filter1
parameters:
  - name: filter1
    transform:
      type: filter
      filter:
        rules:
        - input: doesntSrcPort
          type: remove_entry_if_doesnt_exist
`
const testConfigTransformFilterRemoveEntryIfEqual = `---
log-level: debug
pipeline:
  - name: filter1
parameters:
  - name: filter1
    transform:
      type: filter
      filter:
        rules:
        - input: message
          type: remove_entry_if_equal
          value: "test message"
        - input: value
          type: remove_entry_if_equal
          value: 8.0
`

const testConfigTransformFilterRemoveEntryIfNotEqual = `---
log-level: debug
pipeline:
  - name: filter1
parameters:
  - name: filter1
    transform:
      type: filter
      filter:
        rules:
        - input: message
          type: remove_entry_if_not_equal
          value: "test message"
`

const testConfigTransformFilterAddField = `---
log-level: debug
pipeline:
  - name: filter1
parameters:
  - name: filter1
    transform:
      type: filter
      filter:
        rules:
        - input: dstPort
          type: add_field_if_doesnt_exist
          value: dummy_value
        - input: dummy_field
          type: add_field_if_doesnt_exist
          value: dummy_value
`

func getFilterExpectedOutput() config.GenericMap {
	return config.GenericMap{
		"srcIP":        "10.0.0.1",
		"8888IP":       "8.8.8.8",
		"emptyIP":      "",
		"level":        "error",
		"protocol":     "tcp",
		"protocol_num": 6,
		"value":        7.0,
		"message":      "test message",
		"dstIP":        "20.0.0.2",
	}
}

func TestNewTransformFilterRemoveField(t *testing.T) {
	newTransform := InitNewTransformFilter(t, testConfigTransformFilterRemoveField)
	transformFilter := newTransform.(*Filter)
	require.Len(t, transformFilter.Rules, 2)

	input := test.GetIngestMockEntry(false)
	output, ok := transformFilter.Transform(input)
	require.True(t, ok)
	expectedOutput := getFilterExpectedOutput()
	require.Equal(t, expectedOutput, output)
}

func TestNewTransformFilterRemoveEntryIfExists(t *testing.T) {
	newTransform := InitNewTransformFilter(t, testConfigTransformFilterRemoveEntryIfExists)
	transformFilter := newTransform.(*Filter)
	require.Len(t, transformFilter.Rules, 1)

	input := test.GetIngestMockEntry(false)
	_, ok := transformFilter.Transform(input)
	require.False(t, ok)
}

func TestNewTransformFilterRemoveEntryIfDoesntExists(t *testing.T) {
	newTransform := InitNewTransformFilter(t, testConfigTransformFilterRemoveEntryIfDoesntExists)
	transformFilter := newTransform.(*Filter)
	require.Len(t, transformFilter.Rules, 1)

	input := test.GetIngestMockEntry(false)
	_, ok := transformFilter.Transform(input)
	require.False(t, ok)
}

func TestNewTransformFilterRemoveEntryIfEqual(t *testing.T) {
	newTransform := InitNewTransformFilter(t, testConfigTransformFilterRemoveEntryIfEqual)
	transformFilter := newTransform.(*Filter)
	require.Len(t, transformFilter.Rules, 2)

	input := test.GetIngestMockEntry(false)

	_, ok := transformFilter.Transform(input)
	require.False(t, ok)

	input["message"] = "dummy message"
	output, ok := transformFilter.Transform(input)
	require.True(t, ok)
	require.Equal(t, output["message"], "dummy message")

	input["value"] = 8.0
	_, ok = transformFilter.Transform(input)
	require.False(t, ok)
}

func TestNewTransformFilterRemoveEntryIfNotEqual(t *testing.T) {
	newTransform := InitNewTransformFilter(t, testConfigTransformFilterRemoveEntryIfNotEqual)
	transformFilter := newTransform.(*Filter)
	require.Len(t, transformFilter.Rules, 1)

	input := test.GetIngestMockEntry(false)

	output, ok := transformFilter.Transform(input)
	require.True(t, ok)
	require.Equal(t, output["message"], "test message")

	input["message"] = "dummy message"
	_, ok = transformFilter.Transform(input)
	require.False(t, ok)
}

func TestNewTransformFilterAddField(t *testing.T) {
	newTransform := InitNewTransformFilter(t, testConfigTransformFilterAddField)
	transformFilter := newTransform.(*Filter)
	require.Len(t, transformFilter.Rules, 2)

	input := test.GetIngestMockEntry(false)
	output, ok := transformFilter.Transform(input)
	require.True(t, ok)
	require.Equal(t, 22, output["dstPort"])
	require.Equal(t, "dummy_value", output["dummy_field"])

	input = test.GetIngestMockEntry(false)
	input["dstPort"] = 3490
	input["dummy_field"] = 1
	output, ok = transformFilter.Transform(input)
	require.True(t, ok)
	require.Equal(t, 3490, output["dstPort"])
	require.Equal(t, 1, output["dummy_field"])
}

func InitNewTransformFilter(t *testing.T, configFile string) Transformer {
	cfg := test.InitConfig(t, configFile)

	config := cfg.Parameters[0]
	newTransform, err := NewTransformFilter(config)
	require.NoError(t, err)
	return newTransform
}

func Test_Transform_AddIfScientificNotation(t *testing.T) {
	newFilter := Filter{
		Rules: []api.TransformFilterRule{
			{
				Input:      "value",
				Output:     "bigger_than_10",
				Type:       "add_field_if",
				Parameters: ">10",
			},
			{
				Input:      "value",
				Output:     "smaller_than_10",
				Type:       "add_field_if",
				Parameters: "<10",
			},
			{
				Input:      "value",
				Output:     "dir",
				Assignee:   "in",
				Type:       "add_field_if",
				Parameters: "==1",
			},
			{
				Input:      "value",
				Output:     "dir",
				Assignee:   "out",
				Type:       "add_field_if",
				Parameters: "==0",
			},
			{
				Input:      "value",
				Output:     "not_one",
				Assignee:   "true",
				Type:       "add_field_if",
				Parameters: "!=1",
			},
			{
				Input:      "value",
				Output:     "not_one",
				Assignee:   "false",
				Type:       "add_field_if",
				Parameters: "==1",
			},
		},
	}

	var entry config.GenericMap
	entry = config.GenericMap{
		"value": 1.2345e67,
	}
	output, ok := newFilter.Transform(entry)
	require.True(t, ok)
	require.Equal(t, true, output["bigger_than_10_Evaluate"])
	require.Equal(t, 1.2345e67, output["bigger_than_10"])
	require.Equal(t, "true", output["not_one"])

	entry = config.GenericMap{
		"value": 1.2345e-67,
	}
	output, ok = newFilter.Transform(entry)
	require.True(t, ok)
	require.Equal(t, true, output["smaller_than_10_Evaluate"])
	require.Equal(t, 1.2345e-67, output["smaller_than_10"])
	require.Equal(t, "true", output["not_one"])

	entry = config.GenericMap{
		"value": 1,
	}
	output, ok = newFilter.Transform(entry)
	require.True(t, ok)
	require.Equal(t, true, output["dir_Evaluate"])
	require.Equal(t, "in", output["dir"])
	require.Equal(t, "false", output["not_one"])

	entry = config.GenericMap{
		"value": 0,
	}
	output, ok = newFilter.Transform(entry)
	require.True(t, ok)
	require.Equal(t, true, output["dir_Evaluate"])
	require.Equal(t, "out", output["dir"])
	require.Equal(t, "true", output["not_one"])
}

func Test_TransformFilterDependentRulesAddRegExIf(t *testing.T) {
	var yamlConfig = []byte(`
log-level: debug
pipeline:
  - name: transform1
  - name: write1
    follows: transform1
parameters:
  - name: transform1
    transform:
      type: filter
      filter:
        rules:
        - input: subnetSrcIP
          type: add_field_if_doesnt_exist
          value: 10.0.0.0/24
        - input: subnetSrcIP
          output: match-10.0.*
          type: add_regex_if
          parameters: 10.0.*
        - input: subnetSrcIP
          output: match-11.0.*
          type: add_regex_if
          parameters: 11.0.*
  - name: write1
    write:
      type: stdout
`)
	newFilter := InitNewTransformFilter(t, string(yamlConfig)).(*Filter)
	require.NotNil(t, newFilter)

	entry := test.GetIngestMockEntry(false)
	output, ok := newFilter.Transform(entry)
	require.True(t, ok)

	require.Equal(t, "10.0.0.1", output["srcIP"])
	require.Equal(t, "10.0.0.0/24", output["subnetSrcIP"])
	require.Equal(t, "10.0.0.0/24", output["match-10.0.*"])
	require.NotEqual(t, "10.0.0.0/24", output["match-11.0.*"])
}

func Test_AddLabelIf(t *testing.T) {
	entry := config.GenericMap{
		"param1": 5,
		"param2": 7,
		"param3": -1,
	}
	cfg := config.StageParam{
		Transform: &config.Transform{
			Filter: &api.TransformFilter{
				Rules: []api.TransformFilterRule{
					{
						Type:       "add_label_if",
						Input:      "param1",
						Parameters: "<10",
						Output:     "group1",
						Assignee:   "LT10",
					},
					{
						Type:       "add_label_if",
						Input:      "param1",
						Parameters: ">=10",
						Output:     "group1",
						Assignee:   "GE10",
					},
					{
						Type:       "add_label_if",
						Input:      "param2",
						Parameters: "<5",
						Output:     "group2",
						Assignee:   "LT5",
					},
					{
						Type:       "add_label_if",
						Input:      "param3",
						Parameters: "<0",
						Output:     "group3",
						Assignee:   "LT0",
					},
				},
			},
		},
	}

	tr, err := NewTransformFilter(cfg)
	require.NoError(t, err)

	output, ok := tr.Transform(entry)
	require.True(t, ok)
	require.Contains(t, output, "labels")
	labelsString := output["labels"]
	require.Contains(t, labelsString, "group1=LT10")
	require.Contains(t, labelsString, "group3=LT0")
	require.NotContains(t, labelsString, "group2")
	require.NotContains(t, labelsString, "group1=GE10")
}

func Test_AddLabel(t *testing.T) {
	entry := config.GenericMap{
		"param1": 5,
		"param2": 7,
		"param3": -1,
	}
	cfg := config.StageParam{
		Transform: &config.Transform{
			Filter: &api.TransformFilter{
				Rules: []api.TransformFilterRule{
					{
						Type:  "add_label",
						Input: "key1",
						Value: "value1",
					},
					{
						Type:  "add_label",
						Input: "key2",
						Value: "value2",
					},
					{
						Type:  "add_label",
						Input: "key3",
						Value: "value3",
					},
				},
			},
		},
	}

	tr, err := NewTransformFilter(cfg)
	require.NoError(t, err)

	output, ok := tr.Transform(entry)
	require.True(t, ok)
	require.Contains(t, output, "labels")
	labelsString := output["labels"]
	require.Contains(t, labelsString, "key1=value1")
	require.Contains(t, labelsString, "key2=value2")

	cfg2 := config.StageParam{
		Transform: &config.Transform{
			Filter: &api.TransformFilter{},
		},
	}

	tr, err = NewTransformFilter(cfg2)
	require.NoError(t, err)

	output, ok = tr.Transform(entry)
	require.True(t, ok)
	require.NotContains(t, output, "labels")
}

func Test_AddField(t *testing.T) {
	entry := config.GenericMap{
		"param1": 5,
		"param2": 7,
		"param3": -1,
	}
	cfg := config.StageParam{
		Transform: &config.Transform{
			Filter: &api.TransformFilter{
				Rules: []api.TransformFilterRule{
					{
						Type:  "add_field",
						Input: "field1",
						Value: "value1",
					},
					{
						Type:  "add_field",
						Input: "param1",
						Value: "new_value",
					},
				},
			},
		},
	}

	tr, err := NewTransformFilter(cfg)
	require.NoError(t, err)

	output, ok := tr.Transform(entry)
	require.True(t, ok)
	require.Contains(t, output, "field1")
	require.Equal(t, "value1", output["field1"])
	require.Equal(t, "new_value", output["param1"])
}
