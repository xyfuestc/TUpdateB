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
var curPolicy = 0
var NumOfMB = 1 //以这个为准，会同步到各个节点
var traceName = "hm_0"
var XOROutFilePath = "../request/"+traceName+"_"+strconv.Itoa(NumOfMB)+"M.csv.txt"
var RSOutFilePath = "../request/"+traceName+"_"+strconv.Itoa(NumOfMB*config.W)+"M.csv.txt"
var OutFilePath = XOROutFilePath
var actualUpdatedBlocks = 0
var sidCounter = 0
var beginTime time.Time
var endTime time.Time
var totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
var finished = false
var connections []net.Conn
func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(&ack)
	if schedule.GetCurPolicy().IsFinished() {
		fmt.Printf("=====================================\n")
		endTime = time.Now()
		sumTime := endTime.Unix() - beginTime.Unix()
		throughput :=  float32(numOfReq) * ( float32(config.BlockSize) / config.Megabyte) / float32(sumTime)
		actualUpdatedBlocks = schedule.GetCurPolicy().GetActualBlocks()
		averageOneUpdateSpeed := float32(sumTime) / float32(actualUpdatedBlocks)
		fmt.Printf("%s 总耗时: %ds, 完成更新任务: %d, 实际处理任务数: %d, 单块更新时间: %0.4fs, 吞吐量: %0.2fMB/s\n",
			config.CurPolicyStr[curPolicy], sumTime, numOfReq, actualUpdatedBlocks, averageOneUpdateSpeed, throughput)

		schedule.GetCurPolicy().Clear()
		clearRound()

	}
}
func clearUpdates() {
	actualUpdatedBlocks = 0
	numOfReq = 0
	finished = true
	totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
}
func clearRound()  {
	finished = true
	actualUpdatedBlocks = 0
}
func main() {
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

	for curPolicy < config.NumOfAlgorithm {
		start()
		//保证主线程运行
		for  {
			if finished {
				finished = false
				break
			}
		}
		curPolicy++
	}
	//清空
	clearUpdates()
}

func setCurrentTrace() {
	//CAURS算法
	if curPolicy == len(config.CurPolicyStr) - 1 {
		OutFilePath = RSOutFilePath
	}
}

func getReqsFromTrace()  {
	//清空totalReqs
	totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)

	blockFile, err := os.Open(OutFilePath)
	//处理block请求
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
		blockID, rangeLeft, rangeRight := 0, 0, config.BlockSize
		if len(userRequestStr) == 1 {
			blockID, _ = strconv.Atoi(userRequestStr[0])
		}else{
			blockID, _ = strconv.Atoi(userRequestStr[0])
			rangeLeft, _ = strconv.Atoi(userRequestStr[1])
			rangeRight, _ = strconv.Atoi(userRequestStr[2])
		}

		req := &config.ReqData{
			BlockID: blockID,
			RangeLeft: rangeLeft,
			RangeRight: rangeRight,
		}
		totalReqs = append(totalReqs, req)

		sidCounter++
	}
	defer blockFile.Close()
	numOfReq = sidCounter
}

func settingCurrentPolicy(policyType int)  {
	p := &config.Policy{
		Type:      policyType,
		NumOfMB:   NumOfMB,
		TraceName: traceName,
	}
	for _, ip := range config.NodeIPs{
		common.SendData(p, ip, config.NodeSettingsListenPort, "")
	}
	fmt.Printf("等待设置完成...\n")
	time.Sleep(3 * time.Second)
}

func start()  {
	setCurrentTrace()
	getReqsFromTrace()

	beginTime = time.Now()
	fmt.Printf(" 设置当前算法：[%s], 当前数据集为：%s, blockSize=%vMB.\n", config.CurPolicyStr[curPolicy], OutFilePath, NumOfMB)
	settingCurrentPolicy(curPolicy)
	fmt.Printf(" [%s]算法开始运行...总共block请求数量为：%d\n", config.CurPolicyStr[curPolicy], sidCounter)
	schedule.SetPolicy(config.PolicyType(curPolicy))
	schedule.GetCurPolicy().HandleReq(totalReqs)
}
func listenACK(listen net.Listener) {

	//清除连接
	defer func() {
		for _, conn := range connections {
			conn.Close()
		}
	}()

	for {
		conn, e := listen.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}
		go handleACK(conn)
		connections = append(connections, conn)
		if len(connections)%100 == 0 {
			log.Printf("total number of connections: %v", len(connections))
		}
	}
}

