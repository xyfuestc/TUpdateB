package main

import (
	common "EC/common"
	"EC/config"
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

/******the structure of one line for the update stream file*******/
const (
	Timestamp int  = iota    // default 0
	WorkloadName    //1
	VolumeID        //2
	OperationType   //3
	AccessOffset    //4
	OperatedSize    //5
	DurationTime    //6
)

type UserRequest struct {
	Timestamp       uint64
	WorkloadName    string
	VolumeID        int
	OperationType   string
	AccessOffset    int
	OperatedSize    int
	DurationTime    int
}

var numOfRequestBlocks = 0
var isWaitingForACK = true
var numOfUpdatedBlocks = 0
var currentTime  = 0
var numOfUserRequests = 0
func main() {
	//go listenACK()
	handleRequestsFromFile("./example-traces/wdev_1.csv")
}
func listenACK() {
	/*****设置监听*****/
	fmt.Printf("listening ack to %s:%s\n",common.GetLocalIP(), config.ClientACKListenPort )
	listen, err := net.Listen("tcp", common.GetLocalIP() + ":" +config.ClientACKListenPort)
	if err != nil {
		fmt.Printf("listen failed, err:%v", err)
		return
	}
	/*****等待连接******/
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Printf("accept failed, err:%v", err)
			continue
		}
		go handleACK(conn)  //启动一个单独的goroutine去处理连接
	}
}
func handleRequestsFromFile(fileName string) {
	currentTime := time.Now()   //record current time

	updateStreamFile, _ := openFile(fileName)
	userRequestGroup := getUpdateRequestFromFile(updateStreamFile)
	blockGroup := turnRequestsToBlocks(userRequestGroup)
	handleBlockGroup(blockGroup)
	//waitForACK()

	fmt.Printf("Total request num is %d, request data blocks are %d, spending time is: %v,%vs\n",
		numOfUserRequests, numOfRequestBlocks, time.Now(), currentTime )
	fmt.Printf("Client is finished.\n")
}

func waitForACK() {
	for{
		if !isWaitingForACK {
			break
		}
	}
}

func handleBlockGroup(blockGroup []int) {
	for _, blockID := range blockGroup {
		requestBlockToMS(blockID)
		//fmt.Printf("%d,Node:%d,section:%d \n", i, i/config.W, i%config.W)
		//updateLocalData(metaInfo)
	}
}

func turnRequestsToBlocks(userRequestGroup []UserRequest) []int {
	blockGroup := make([]int, 0, 100000)
	for _, userRequest := range userRequestGroup {
		stripeID := userRequest.AccessOffset / (config.K*config.W*config.ChunkSize)
		fmt.Printf("stripeID:%d\n",stripeID)
		start := userRequest.AccessOffset % (config.K*config.W*config.ChunkSize)
		end := (userRequest.AccessOffset+userRequest.OperatedSize) % (config.K*config.W*config.ChunkSize)
		minBlockID := (stripeID*config.K*config.W + start) / config.ChunkSize
		maxBlockID := (stripeID*config.K*config.W + end) / config.ChunkSize
		for i := minBlockID; i <= maxBlockID; i++ {
			blockGroup = append(blockGroup, i)
		}
	}
	return blockGroup
}

func openFile(fileName string) (*os.File, error) {
	fmt.Printf("read update stream file: %s\n", fileName)
	updateStreamFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("Error: ", err)
	}
	defer updateStreamFile.Close()
	return updateStreamFile, err
}
func getUpdateRequestFromFile(file *os.File) []UserRequest {
	userRequestGroup := make([]UserRequest, 0, 100000)
	/*******read the user requests line by line*********/
	bufferReader := bufio.NewReader(file)
	for {
		lineData, _, err := bufferReader.ReadLine()
		if err == io.EOF {
			break
		}
		userRequestGroup = append(userRequestGroup, getOneRequestFromOneLine(lineData))
	}
	return userRequestGroup
}

func getOneRequestFromOneLine(lineData []byte) UserRequest {
	numOfUserRequests++ //update the number of user requests (one line for offSet user request)
	userRequestStr := strings.Split(string(lineData), ",")
	offSet, _ := strconv.Atoi(userRequestStr[AccessOffset])
	readSize, _ := strconv.Atoi(userRequestStr[OperatedSize])
	userRequest := UserRequest{AccessOffset: offSet, OperatedSize: readSize}

	return userRequest
}

func requestBlockToMS(blockID int)  {
	fmt.Printf("connect to ms : %s\n", config.MSIP)
	request := &config.ReqData{
		OPType:   config.UpdateReq,
		BlockID:  blockID,
		StripeID: common.GetStripeIDFromBlockID(blockID),
	}
	common.SendData(request, config.MSIP, config.MSListenPort, "metaInfo")
}
/*********inform datanode to update its local data***********/
func updateLocalData(metaInfo config.MetaInfo) {
	numOfRequestBlocks++    //update the number of request blocks

	fmt.Printf("inform datanode %d to update its local datachunk %d.\n",
												metaInfo.DataNodeID, metaInfo.BlockID)
	//generate random update data
	dataStr := common.RandStringBytesMask(config.ChunkSize)
	dataBytes := []byte(dataStr)
	td := &config.TD{
		OPType:  config.UpdateReq,
		Buff:    dataBytes,
		BlockID: metaInfo.BlockID,
	}
	//send data to datanode for update
	fmt.Printf("send datatype to datanode %d, IP address: %s\n", metaInfo.BlockID, metaInfo.BlockIP)
	common.SendData(td, metaInfo.BlockIP, config.NodeListenPort, "ack")
}
/*******cau********/
func handleACK(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var ack config.Ack
	err := dec.Decode(&ack)
	if err != nil {
		log.Fatal("client handleACK error: ", err)
	}
	fmt.Printf("client receiving ack: %d of updating chunk :%d\n",ack.AckID, ack.ChunkID)
	numOfUpdatedBlocks++
	if numOfUpdatedBlocks == numOfRequestBlocks {
		isWaitingForACK = false
	}
}
