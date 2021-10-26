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
var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var round = 0
var actualUpdatedBlocks = 0
var sidCounter = 0
var beginTime time.Time
var endTime time.Time
var totalBlocks = make([]int, 0, 1000000)
var finished bool = false

func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(ack)
	if isFinished() {
		fmt.Printf("=====================================\n")
		fmt.Printf("结束!\n")
		endTime = time.Now()
		fmt.Printf("总请求数: %d, 用时: %ds\n", numOfReq,
											endTime.Unix() - beginTime.Unix())
		clearUpdates()
		schedule.GetCurPolicy().Clear()

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
	numOfReq = 0
	finished = true
	totalBlocks = make([]int, 0, 1000000)
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

		totalBlocks = append(totalBlocks, blockID)


		sidCounter++
	}
	defer blockFile.Close()
	numOfReq = sidCounter
	fmt.Printf("总共block请求数量为：%d\n", sidCounter)
	schedule.GetCurPolicy().HandleReq(totalBlocks)
	//保证主线程运行


	for  {
		if finished {
			break
		}
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

func isFinished() bool {
	return schedule.ACKIsEmpty()
}