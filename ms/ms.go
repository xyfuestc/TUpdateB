package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"bufio"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)
var numOfReq = 0
//var curPolicy = 6
var curPolicy int32 = 1
var NumOfMB float64 = 0.25 //以这个为准，会同步到各个节点
var traceName = "hm_0"
var XOROutFilePath = "../request/"+traceName+"_"+strconv.Itoa(int(NumOfMB))+"M.csv.txt"
var RSOutFilePath = "../request/"+traceName+"_"+strconv.Itoa( int(NumOfMB * float64(config.W)) )+"M.csv.txt"
var OutFilePath = XOROutFilePath
var actualUpdatedBlocks = 0
var beginTime time.Time
var totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
var roundFinished int32 = 0  // 1-本轮结束 ； 0-本轮未结束
var curNumOfMB float64 = 0
//var connections []net.Conn
//var receivedAckCh = make(chan config.ACK, 10)
//var wg sync.WaitGroup
func checkFinish() {

	isRoundFinished := atomic.LoadInt32(&roundFinished)
	curPolicyVal := atomic.LoadInt32(&curPolicy)

	if isRoundFinished == 0 && schedule.GetCurPolicy().IsFinished() && curPolicyVal < config.NumOfAlgorithm {

		//本轮结束
		atomic.StoreInt32(&roundFinished, 1)
		//清空ACK
		schedule.ClearChannels()

		sumTime := time.Since(beginTime)
		throughput :=  float64(numOfReq) * float64(curNumOfMB) / float64(sumTime/time.Second)
		actualUpdatedBlocks = schedule.GetCurPolicy().GetActualBlocks()
		averageOneUpdateSpeed := float64(sumTime/time.Millisecond) / float64(actualUpdatedBlocks) / 1000
		crossTraffic := schedule.GetCrossRackTraffic()
		log.Printf("%s 总耗时: %.2fs, 完成更新任务: %d, 实际处理任务数: %d, 单块更新时间: %0.2fs, 吞吐量: %0.2fMB/s，跨域流量为：%0.2fMB\n",
			config.Policies[curPolicyVal], sumTime.Seconds(), numOfReq, actualUpdatedBlocks, averageOneUpdateSpeed, throughput, crossTraffic)


		schedule.GetCurPolicy().Clear()
		clearRound()
	}
}
/*所有算法跑完，清空操作*/
func clearAll() {
	log.Printf("清空所有数据和资源...\n")
	//schedule.CloseAllChannels()
	actualUpdatedBlocks = 0
	numOfReq = 0
}
/*每种算法结束后，清空操作*/
func clearRound()  {
	actualUpdatedBlocks = 0
}
func main() {
	//defer profile.Start(profile.MemProfile, profile.MemProfileRate(1)).Stop()
	//初始化
	config.Init()

	//监听ack
	log.Printf("ms启动...")
	log.Printf("监听ack: %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)

	//当发生意外退出时，释放所有资源
	registerSafeExit()
	//监听并接收ack，检测程序结束
	listenAndReceive(config.NumOfWorkers)

	getReqsFromTrace()
	curPolicyVal := atomic.LoadInt32(&curPolicy)
	for curPolicyVal < config.NumOfAlgorithm {
		start()
		//保证主线程运行
		for  {
			isRoundFinished := atomic.LoadInt32(&roundFinished)
			if isRoundFinished == 1 {
				//进入下一轮
				atomic.AddInt32(&curPolicy, 1)
				break
			}
		}
		curPolicyVal = atomic.LoadInt32(&curPolicy)
		//curPolicy++

	}
	//清空
	clearAll()
	//通知各个节点退出
	notifyNodesQuit()

}
func listenAndReceive(maxWorkers int)  {
	l2, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeACKListenPort)
	if err != nil {
		log.Fatalln("listening ack err in listenAndReceive: ", err)
	}

	go listenACK(l2)

	for i := 0; i < maxWorkers; i++ {
		go msgSorter(schedule.ReceivedAckCh)
	}
}
func setCurrentTrace() {
	//CAURS算法（必须保证CAURS在最后）
	if config.Policies[curPolicy] == "CAURS" {
		OutFilePath = RSOutFilePath
		getReqsFromTrace()
	}
}

func getReqsFromTrace()  {

	totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)

	blockFile, err := os.Open(OutFilePath)
	defer blockFile.Close()
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

	}
	numOfReq = len(totalReqs)
}
func notifyNodesQuit()  {
	log.Printf("通知各个节点结束任务...\n")
	p := &config.Policy{
		Type:      -1,
	}
	for _, ip := range config.NodeIPs{
		common.SendData(p, ip, config.NodeSettingsListenPort)
	}
	log.Printf("等待各个节点清理完成...\n")
	time.Sleep(3 * time.Second)
	log.Printf("退出\n")

}
func settingCurrentPolicy(policyType int32)  {

	UsingMulticast := checkMulti(policyType)
	p := &config.Policy{
		Type:      policyType,
		NumOfMB:   NumOfMB,
		TraceName: traceName,
		Multicast: UsingMulticast,
	}

	config.NumOfMB = int(NumOfMB)
	config.BlockSize = int(NumOfMB * config.Megabyte)
	config.RSBlockSize = int(config.Megabyte * NumOfMB) * config.W

	log.Printf("初始化共享池...\n")
	config.InitBufferPool()

	for _, ip := range config.NodeIPs{
		common.SendData(p, ip, config.NodeSettingsListenPort)
	}
	log.Printf("等待设置完成...\n")
	time.Sleep(2 * time.Second)

	if config.Policies[curPolicy] == "CAURS" {
		curNumOfMB = float64(config.W) * NumOfMB
	}else{
		curNumOfMB = NumOfMB
	}
}

func start()  {

	setCurrentTrace() //专门针对CAURS改变数据源

	//time.Sleep(2 * time.Second)
	beginTime = time.Now()
	settingCurrentPolicy(curPolicy)
	log.Printf(" 设置当前算法：[%s], 当前数据集为：%s, blockSize=%vMB.\n", config.Policies[curPolicy], OutFilePath, curNumOfMB)

	//重置为本轮未结束：0
	atomic.StoreInt32(&roundFinished, 0)

	log.Printf(" [%s]算法开始运行，总共block请求数量为：%d\n", config.Policies[curPolicy], numOfReq)
	schedule.SetPolicy(config.Policies[curPolicy])
	schedule.GetCurPolicy().HandleReq(totalReqs)
}
func msgSorter(receivedAckCh <-chan config.ACK)  {
	for  {
		select {
		case ack := <- receivedAckCh:
			schedule.GetCurPolicy().HandleACK(&ack)
			checkFinish()
		}
	}


}
func listenACK(listen net.Listener) {

	//清除连接
	//defer func() {
	//	for _, conn := range connections {
	//		conn.Close()
	//	}
	//}()

	for {
		conn, e := listen.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err in listenACK: %v", ne)
				continue
			}

			log.Printf("accept err in listenACK: %v", e)
			return
		}

		ack := common.GetACK(conn)
		schedule.ReceivedAckCh <- ack

		//connections = append(connections, conn)
		//if len(connections)%100 == 0 {
		//	log.Printf("total number of connections: %v", len(connections))
		//}
	}
}
func registerSafeExit()  {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			clearAll()
			schedule.GetCurPolicy().Clear()
			//schedule.CloseAllChannels()
			os.Exit(0)
		}
	}()
}
func checkMulti(policy int32) bool  {
	UsingMulticast := false
	if policy >= 0 && policy < config.NumOfAlgorithm {
		UsingMulticast = strings.Contains(config.Policies[policy], "Multicast")
	}
	return UsingMulticast

}
