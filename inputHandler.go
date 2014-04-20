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
	replicas []discoInputReplica
}

func newDiscoInputFromParsedJSON(parsed_array []interface{}) discoInput {
	this := discoInput{
		id:     int(parsed_array[0].(float64)),
		status: parsed_array[1].(string),
		label:  int(parsed_array[2].(float64)),
	}

	this.replicas = make([]discoInputReplica, len(parsed_array[3].([]interface{})))

	for idx, replicaArray := range parsed_array[3].([]interface{}) {
		this.replicas[idx] = newDiscoInputReplicaFromParsedJSON(replicaArray.([]interface{}))
	}

	return this
}
