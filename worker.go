package godisco

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
)

const DISCO_PROTOCOL_VERSION string = "1.1"

type DiscoWorker struct {
	outputWriter *bufio.Writer
	inputReader  *bufio.Reader
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
	Host      string `json:"host"`
	Master    string `json:"master"`
	JobName   string `json:"jobname"`
	TaskId    string `json:"taskid"`
	Stage     string `json:"stage"`
	Grouping  string `json:"grouping"`
	Group     string `json:"group"`
	DiscoPort string `json:"disco_port"`
	PutPort   string `json:"put_port"`
	DiscoData string `json:"disco_data"`
	DDFSData  string `json:"ddfs_data"`
	JobFile   string `json:"jobfile"`
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

func (this *DiscoWorker) writeWorkerMessage() {
	startupMessage := workerMessage{DISCO_PROTOCOL_VERSION, os.Getpid()}
	startupJSON, _ := json.Marshal(startupMessage)

	this.writePayload(workerPreamble, startupJSON)
}

func NewDiscoWorker() *DiscoWorker {
	this := &DiscoWorker{
		outputWriter: bufio.NewWriter(os.Stderr),
		inputReader:  bufio.NewReader(os.Stdin),
	}

	this.writeWorkerMessage()

	return this
}
