package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
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
var numOfReq = 0
var numOfACK = 0
var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var round = 0
var actualUpdatedBlocks = 0
var sidCounter = 0
var beginTime time.Time
var endTime time.Time
var totalBlocks = make([]config.ReqData, 0, 1000000)

func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	numOfACK++
	schedule.GetCurPolicy().HandleACK(ack)
	if schedule.RequireACKs == 0 {
		fmt.Printf("=====================================")
		fmt.Printf("结束!")
		endTime = time.Now()
		fmt.Printf("Total request: %d, spend time: %ds\n", numOfReq,
											endTime.Unix() - beginTime.Unix())
		clearUpdates()
	}
}
func PrintGenMatrix(gm []byte)  {

	fmt.Printf("Generation Matrix : \n[")
	for i := 0; i < config.M; i++ {
		for j := 0; j < config.K; j++ {
			fmt.Printf("%d ", gm[i*config.K+j])

			if i==config.M-1 && j==config.K-1 {
				fmt.Printf("%d]", gm[i*config.K+j])
			}
		}
		fmt.Println()
	}
}
func clearUpdates() {
	fmt.Printf("clear ack, req...\n")
	numOfACK = 0
	numOfReq = 0
}
func main() {
	beginTime = time.Now()
	//初始化
	config.Init()

	//监听ack
	fmt.Printf("ms启动...")
	fmt.Printf("监听ack: %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	l2, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeACKListenPort)
	if err != nil {
		log.Fatalln("listening ack err: ", err)
	}
	go listenACK(l2)

	//处理block请求
	blockFile, err := os.Open(config.OutFilePath)
	if err != nil {
		log.Fatalln("Error: ", err)
	}

	bufferReader := bufio.NewReader(blockFile)
	for {
		lineData, _, err := bufferReader.ReadLine()
		if err == io.EOF {
			break
		}else if err != nil {
			log.Fatalln("handleReqFile error: ",err)
		}
		userRequestStr := strings.Split(string(lineData), ",")
		blockID, _ := strconv.Atoi(userRequestStr[0])
		request := config.ReqData{
			SID:      sidCounter,
			BlockID:  blockID,
			StripeID: common.GetStripeIDFromBlockID(blockID),
		}
		totalBlocks = append(totalBlocks, request)


		sidCounter++
	}
	defer blockFile.Close()

	fmt.Printf("总共block请求数量为：%d\n", sidCounter)
	schedule.GetCurPolicy().HandleReq(totalBlocks)
	//保证主线程运行
	for  {
		
	}
	
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