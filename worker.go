package godisco

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"strings"
)

const DISCO_PROTOCOL_VERSION string = "1.1"

type discoWorker struct {
	outputWriter  *bufio.Writer
	inputReader   *bufio.Reader
	taskInfo      *taskMessage
	currentInputs []discoInput
	endOfInput    bool
}

var (
	workerPreamble   = []byte("WORKER")
	taskPreamble     = []byte("TASK")
	inputPreamble    = []byte("INPUT")
	inputErrPreamble = []byte("INPUT_ERR")
	msgPreamble      = []byte("MSG")
	outputPreamble   = []byte("OUTPUT")
	donePreamble     = []byte("DONE")
	errorPreable     = []byte("ERROR")
	fatalPreable     = []byte("FATAL")
	pingPreable      = []byte("PING")
	spaceDelimiter   = []byte(" ")
	lineDelimiter    = []byte("\n")
)

type workerMessage struct {
	Version string `json:"version"`
	Pid     int    `json:"pid"`
}

type taskMessage struct {
	Host      string        `json:"host"`
	Master    string        `json:"master"`
	JobName   string        `json:"jobname"`
	TaskId    int           `json:"taskid"`
	Stage     string        `json:"stage"`
	Grouping  string        `json:"grouping"`
	Group     []interface{} `json:"group"`
	DiscoPort int           `json:"disco_port"`
	PutPort   int           `json:"put_port"`
	DiscoData string        `json:"disco_data"`
	DDFSData  string        `json:"ddfs_data"`
	JobFile   string        `json:"jobfile"`
}

func (this *discoWorker) writePayload(preamble []byte, payload []byte) {
	payloadLen := []byte(strconv.Itoa(len(payload)))

	this.outputWriter.Write(preamble)
	this.outputWriter.Write(spaceDelimiter)
	this.outputWriter.Write(payloadLen)
	this.outputWriter.Write(spaceDelimiter)
	this.outputWriter.Write(payload)
	this.outputWriter.Write(lineDelimiter)
	this.outputWriter.Flush()
}

func (this *discoWorker) writeJSONPayload(preamble []byte, payload interface{}) {
	jsonResult, _ := json.Marshal(payload)
	this.writePayload(preamble, jsonResult)
}

func (this *discoWorker) sendAndReceive(preamble []byte, payload interface{}) {
	this.writeJSONPayload(preamble, payload)
	response, _, _ := this.inputReader.ReadLine()
	this.handleResponse(response)
}

func (this *discoWorker) Debug(text string) {
	this.sendAndReceive(msgPreamble, text)
}

func (this *discoWorker) writeWorkerMessage() {
	startupMessage := workerMessage{DISCO_PROTOCOL_VERSION, os.Getpid()}

	this.sendAndReceive(workerPreamble, startupMessage)
}

func (this *discoWorker) getTask() {
	this.sendAndReceive(taskPreamble, "")
}

func (this *discoWorker) getInput() {
	this.sendAndReceive(inputPreamble, "")
}

func (this *discoWorker) notifyOutput(input discoInput, outputLocation string, outputSize int) {
	payload := make([]interface{}, 3)
	payload[1] = outputLocation
	payload[2] = outputSize

	if input.labelAll {
		payload[0] = "all"
	} else {
		payload[0] = input.label
	}

	this.writeJSONPayload(outputPreamble, payload)
}

func (this *discoWorker) parseResponse(response []byte) (code string, length string, payload string) {
	responseStr := string(response)
	parseResult := strings.SplitN(responseStr, string(spaceDelimiter), 3)
	return parseResult[0], parseResult[1], parseResult[2]
}

func (this *discoWorker) handleResponse(response []byte) {
	code, _, payload := this.parseResponse(response)
	switch code {
	case "OK":
		// nothing to do, thanks?
	case "TASK":
		err := json.Unmarshal([]byte(payload), this.taskInfo)
		if err != nil {
			panic(err)
		}

	case "INPUT":
		var inputMessage []interface{}
		err := json.Unmarshal([]byte(payload), &inputMessage)
		if err != nil {
			panic(err)
		}

		if inputMessage[0].(string) == "done" {
			this.endOfInput = true
		}

		inputsToProcess := inputMessage[1].([]interface{})

		this.currentInputs = make([]discoInput, len(inputsToProcess))

		for idx, elm := range inputsToProcess {
			this.currentInputs[idx] = newDiscoInputFromParsedJSON(elm.([]interface{}))
		}

		this.notifyOutput(this.currentInputs[0], "disco://localhost/ddfs/vol0/blob/49/tiny_file_txt$575-e7543-98a54", 55)

	default:
		//panic("NOT IMPLEMENTED")
		this.Debug(string(response))
	}
}

func NewDiscoWorker() *discoWorker {
	this := &discoWorker{
		outputWriter: bufio.NewWriter(os.Stderr),
		inputReader:  bufio.NewReader(os.Stdin),
		taskInfo:     new(taskMessage),
		endOfInput:   false,
	}

	this.writeWorkerMessage()
	this.getTask()
	this.getInput()

	return this
}
