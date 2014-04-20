package godisco

type discoInputReplica struct {
	id     int
	source string
}

func newDiscoInputReplicaFromParsedJSON(parsed_array []interface{}) discoInputReplica {
	this := discoInputReplica{
		id:     int(parsed_array[0].(float64)),
		source: parsed_array[1].(string),
	}

	return this
}

type discoInput struct {
	id       int
	status   string
	label    int
	labelAll bool
	replicas []discoInputReplica
}

func newDiscoInputFromParsedJSON(parsed_array []interface{}) discoInput {
	// if label is 'all' we set label in the struct to -1 and labelAll to true
	var label int
	var labelAll bool

	switch v := parsed_array[2].(type) {
	case string:
		if v != "all" {
			panic("UNEXPECTED LABEL")
		}
		label = -1
		labelAll = true
	case float64:
		label = int(v)
		labelAll = false
	default:
		panic("UNEXPECTED LABEL TYPE")
	}

	this := discoInput{
		id:       int(parsed_array[0].(float64)),
		status:   parsed_array[1].(string),
		label:    label,
		labelAll: labelAll,
	}

	this.replicas = make([]discoInputReplica, len(parsed_array[3].([]interface{})))

	for idx, replicaArray := range parsed_array[3].([]interface{}) {
		this.replicas[idx] = newDiscoInputReplicaFromParsedJSON(replicaArray.([]interface{}))
	}

	return this
}
