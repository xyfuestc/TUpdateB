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
var round = 0
var actualUpdatedBlocks = 0
var sidCounter = 0
var beginTime time.Time
var endTime time.Time
var totalBlocks = make([]int, 0, config.MaxBlockSize)
var finished bool = false

func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(&ack)
	if schedule.GetCurPolicy().IsFinished() {
		fmt.Printf("=====================================\n")
		endTime = time.Now()
		sumTime := endTime.Unix() - beginTime.Unix()
		averageOneUpdateSpeed := float32(sumTime) / float32(numOfReq)
		throughput :=  float32(numOfReq) * ( float32(config.BlockSize) / config.Megabyte) / float32(sumTime)
		actualUpdatedBlocks = schedule.GetActualBlocks()
		fmt.Printf("%s 总耗时: %ds, 完成更新任务: %d, 实际处理任务数: %d, 单个更新速度: %0.4fs, 吞吐量: %0.2f个/s\n",
			config.CurPolicyStr[config.CurPolicyVal], sumTime, numOfReq, actualUpdatedBlocks, averageOneUpdateSpeed, throughput)

		schedule.GetCurPolicy().Clear()
		clearRound()

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
	actualUpdatedBlocks = 0
	numOfReq = 0
	finished = true
	totalBlocks = make([]int, 0, config.MaxBlockSize)
}
func clearRound()  {
	finished = true
	actualUpdatedBlocks = 0
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
	for i := 0; i < config.NumOfAlgorithm; i++ {
		//启动运行
		start(i)
		//保证主线程运行
		for  {
			if finished {
				finished = false
				break
			}
		}
	}
	//清空
	clearUpdates()
}

func settingCurrentPolicy(policyType int)  {
	p := &config.Policy{
		Type: policyType,
	}
	for _, ip := range config.NodeIPs{
		common.SendData(p, ip, config.NodeSettingsListenPort, "")
	}
	fmt.Printf("等待设置完成...")
	time.Sleep(10 * time.Second)
}

func start(round int)  {
	fmt.Printf(" 设置当前算法：[%s]\n", config.CurPolicyStr[round])
	settingCurrentPolicy(round)
	fmt.Printf(" [%s]算法开始运行...总共block请求数量为：%d\n", config.CurPolicyStr[round], sidCounter)
	schedule.SetPolicy(config.PolicyType(round))
	schedule.GetCurPolicy().HandleReq(totalBlocks)
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

