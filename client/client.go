package main

import (
	common "EC/common"
	"EC/config"
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)
var numOfRequestBlocks = 0
var isWaitingForACK = true
var numOfUpdatedBlocks = 0
var numOfUserRequests = 0
var sidCounter = 0
func main() {
	config.Init()

	config.BeginTime = time.Now()
	fmt.Printf("%s : simulation start\n", config.BeginTime.Format("2010-01-02 15:04:02"))
	handleRequestsFromFile("./example-traces/test.csv")
}
func handleRequestsFromFile(fileName string) {
	updateStreamFile,_ := openFile(fileName)
	handleReqFile(updateStreamFile)
}
func openFile(fileName string) (*os.File, error) {
	fmt.Printf("reading update stream file: %s\n", fileName)
	updateStreamFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("Error: ", err)
	}
	return updateStreamFile, err
}
func handleReqFile(file *os.File) {
	bufferReader := bufio.NewReader(file)
	for {
		lineData, _, err := bufferReader.ReadLine()
		if err == io.EOF {
			break
		}else if err != nil {
			log.Fatalln("handleReqFile error: ",err)
		}
		request := getOneRequestFromOneLine(lineData)
		minBlockID, maxBlockID := common.GetBlocksFromOneRequest(request)
		for i := minBlockID; i <= maxBlockID; i++ {
			requestBlockToMS(i)
		}
	}
	defer file.Close()
}
func getOneRequestFromOneLine(lineData []byte) config.UserRequest {
	numOfUserRequests++ //update the number of user requests (one line for offSet user request)
	userRequestStr := strings.Split(string(lineData), ",")
	offSet, _ := strconv.Atoi(userRequestStr[config.AccessOffset])
	readSize, _ := strconv.Atoi(userRequestStr[config.OperatedSize])
	userRequest := config.UserRequest{AccessOffset: offSet, OperatedSize: readSize}

	return userRequest
}
func requestBlockToMS(blockID int)  {
	fmt.Printf("sid %d : request block %d to ms : %s\n", sidCounter, blockID, config.MSIP)
	request := &config.ReqData{
		SID:      sidCounter,
		OPType:   config.UpdateReq,
		BlockID:  blockID,
		StripeID: common.GetStripeIDFromBlockID(blockID),
	}
	common.SendData(request, config.MSIP, config.NodeReqListenPort, "metaInfo")

	sidCounter++
}
func listenACK(listen net.Listener) {
	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatalln("listenACK  err: ", err)
		}
		go handleACK(conn)
	}
}

func handleACK(conn net.Conn) {
	ack := common.GetACK(conn)
	fmt.Printf("receive ms' ack : %v\n", ack)
}