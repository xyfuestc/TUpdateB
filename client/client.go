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
	"path/filepath"
	"strconv"
	"strings"
)
var numOfRequestBlocks = 0
var isWaitingForACK = true
var numOfUpdatedBlocks = 0
var numOfUserRequests = 0
const outFilePre = "./request/"
const outFileSuffix = ".txt"
const RequestFileName = "./example-traces/wdev_1.csv"

func main() {

	generateRequestBlocksFromFile(RequestFileName)

	//config.Init()
	//
	//fmt.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	//l1, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeACKListenPort)
	//if err != nil {
	//	fmt.Printf("listening ack failed, err:%v\n", err)
	//	return
	//}
	//go listenACK(l1)
	//
	//config.BeginTime = time.Now()
	//fmt.Printf("%s : simulation start\n", config.BeginTime.Format("2010-01-02 15:04:02"))
	//handleRequestsFromFile("../example-traces/wdev_1.csv")
	//for  {
	//
	//}
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
func checkFileIsExist(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}
func handleReqFile(file *os.File) {
	bufferReader := bufio.NewReader(file)
	OutFilePath := outFilePre + filepath.Base(RequestFileName) + outFileSuffix
	var blockFile *os.File
	if checkFileIsExist(OutFilePath) { //如果文件存在
		//blockFile, _ = os.OpenFile(FileName, os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0666)
		blockFile, _ = os.OpenFile(OutFilePath, os.O_TRUNC|os.O_WRONLY, 0666)
		fmt.Println("文件存在")
	} else {
		blockFile, _ = os.Create(OutFilePath) //创建文件
		fmt.Println("文件不存在")
	}
	write := bufio.NewWriter(blockFile)
	for {
		lineData, _, err := bufferReader.ReadLine()
		if err == io.EOF {
			break
		}else if err != nil {
			log.Fatalln("handleReqFile error: ",err)
		}
		request := getOneRequestFromOneLine(lineData)
		min_offset := request.AccessOffset % config.BlockSize
		min_size := config.BlockSize - min_offset
		max_size := ( request.AccessOffset + request.OperatedSize ) % config.BlockSize
		offset, size := min_offset, request.OperatedSize
		minBlockID, maxBlockID := common.GetBlocksFromOneRequest(request)
		//只有一个块
		if minBlockID == maxBlockID {
			var str = strconv.Itoa(minBlockID) + ","+strconv.Itoa(min_offset)+","+strconv.Itoa(offset+size)+"\n"
			write.WriteString(str)
		}else { //多个块
			for i := minBlockID; i <= maxBlockID; i++ {
				if i == minBlockID {
					offset = min_offset
					size = min_size
					//requestBlockToMS(i)
				} else if i == maxBlockID {
					offset = 0
					size = max_size
					//requestBlockToMS(i)
				} else {
					offset = 0
					size = config.BlockSize
				}
				var str = strconv.Itoa(i) + "," + strconv.Itoa(offset) + "," + strconv.Itoa(offset+size) + "\n"
				write.WriteString(str)
			}
		}
	}
	write.Flush()
	defer blockFile.Close()
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
	//fmt.Printf("sid %d : request block %d to ms : %s\n", sidCounter, blockID, config.MSIP)
	request := &config.ReqData{
		//SID:      sidCounter,

		BlockID:  blockID,
		StripeID: common.GetStripeIDFromBlockID(blockID),
	}
	common.SendData(request, config.MSIP, config.NodeReqListenPort, "metaInfo")

	//sidCounter++
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

func generateRequestBlocksFromFile(fileName string) {
	updateStreamFile,_ := openFile(fileName)
	handleReqFile(updateStreamFile)
}