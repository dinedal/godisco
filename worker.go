package godisco

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"
)

const DISCO_PROTOCOL_VERSION string = "1.1"

type DiscoWorker struct {
	outputWriter *bufio.Writer
	inputReader  *bufio.Reader
	taskInfo     *taskMessage
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

func (this *DiscoWorker) writePayload(preamble []byte, payload []byte) {
	payloadLen := []byte(strconv.Itoa(len(payload)))

	this.outputWriter.Write(preamble)
	this.outputWriter.Write(spaceDelimiter)
	this.outputWriter.Write(payloadLen)
	this.outputWriter.Write(spaceDelimiter)
	this.outputWriter.Write(payload)
	this.outputWriter.Write(lineDelimiter)
	this.outputWriter.Flush()
}

func (this *DiscoWorker) writeJSONPayload(preamble []byte, payload interface{}) {
	jsonResult, _ := json.Marshal(payload)
	this.writePayload(preamble, jsonResult)
}

func (this *DiscoWorker) sendAndReceive(preamble []byte, payload interface{}) {
	this.writeJSONPayload(preamble, payload)
	response, _, _ := this.inputReader.ReadLine()
	this.handleResponse(response)
}

func (this *DiscoWorker) Debug(text string) {
	this.sendAndReceive(msgPreamble, text)
}

func (this *DiscoWorker) writeWorkerMessage() {
	startupMessage := workerMessage{DISCO_PROTOCOL_VERSION, os.Getpid()}

	this.sendAndReceive(workerPreamble, startupMessage)
}

func (this *DiscoWorker) getTask() {
	this.sendAndReceive(taskPreamble, "")
}

func (this *DiscoWorker) parseResponse(response []byte) (code string, length string, payload string) {
	responseStr := string(response)
	parseResult := strings.SplitN(responseStr, string(spaceDelimiter), 3)
	return parseResult[0], parseResult[1], parseResult[2]
}

func (this *DiscoWorker) handleResponse(response []byte) {
	code, _, payload := this.parseResponse(response)
	switch code {
	case "OK":

	case "TASK":
		err := json.Unmarshal([]byte(payload), this.taskInfo)
		if err != nil {
			panic(err)
		}

	default:
		panic("NOT IMPLEMENTED")
	}
}

func NewDiscoWorker() *DiscoWorker {
	this := &DiscoWorker{
		outputWriter: bufio.NewWriter(os.Stderr),
		inputReader:  bufio.NewReader(os.Stdin),
		taskInfo:     new(taskMessage),
	}

	this.writeWorkerMessage()
	this.getTask()
	for {
		time.Sleep(100 * time.Millisecond)
		input, _, _ := this.inputReader.ReadLine()
		this.Debug(string(input))
	}
	return this
}
