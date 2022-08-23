package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"bufio"
	"flag"
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
//var curPolicy int32 = 2
//var NumOfMB float64 = 0.25 //以这个为准，会同步到各个节点
//var traceName = "rsrch_2"
var OutFilePath = ""
//var RSOutFilePath = "../request/"+traceName+"_"+strconv.Itoa( int(NumOfMB * float64(config.W)) )+"M.csv.txt"
//var OutFilePath = OutFilePath
var SpaceFilePath = "../log/space_log.txt"
var actualUpdatedBlocks = 0
var beginTime time.Time
var totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
var roundFinished int32 = 0  // 1-本轮结束 ； 0-本轮未结束
var ScheduleFinishedChan = make(chan bool, 1)

var sumTime time.Duration = 0
var NumOfMB = flag.Float64("b", 4, "块大小：int型参数，默认：64MB")
var curPolicy = flag.Int("p", 0, "策略ID：0-Base;1-CRRepairBoost;2-Express，默认：0")
var closeNodes = flag.Int("c", 0, "是否程序结束自动关闭各节点？1-关闭；0-不关闭，默认：0")
var traceName = flag.String("f", "rsrch_2", "日志文件名，默认：rsrch_2")
func checkFinish() {

	isRoundFinished := atomic.LoadInt32(&roundFinished)
	p := int32(*curPolicy)
	var curPolicyVal = atomic.LoadInt32(&p)
	if isRoundFinished == 0 && schedule.GetCurPolicy().IsFinished() && curPolicyVal < config.NumOfAlgorithm {

		//清空ACK
		schedule.ClearChannels()

		sumTime = time.Since(beginTime)
		//本轮结束
		atomic.StoreInt32(&roundFinished, 1)

		throughput :=  float64(numOfReq) * float64(*NumOfMB) / sumTime.Seconds()
		actualUpdatedBlocks = schedule.GetCurPolicy().GetActualBlocks()
		averageOneUpdateSpeed := float64(sumTime/time.Millisecond) / float64(actualUpdatedBlocks) / 1000
		crossTraffic := schedule.GetCurPolicy().GetCrossRackTraffic()
		crossTraffic = crossTraffic / float32(numOfReq)
		log.Printf("%s 总耗时: %.2fs, 完成更新任务: %d, 实际处理任务数: %d, 单块更新时间: %0.2fs, 吞吐量: %0.2fMB/s，单块平均跨域流量为：%0.3fMB\n",
			config.Policies[curPolicyVal], sumTime.Seconds(), numOfReq, actualUpdatedBlocks, averageOneUpdateSpeed, throughput, crossTraffic)

		schedule.GetCurPolicy().Clear()
		clearRound()
		//表明本算法结束
		ScheduleFinishedChan <- true
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

	flag.Parse()
	//初始化
	config.Init()

	OutFilePath = "../request/"+*traceName+"_"+strconv.Itoa(int(*NumOfMB))+"M.csv.txt"

	//监听ack
	log.Printf("ms启动...")
	log.Printf("监听ack: %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)

	//当发生意外退出时，释放所有资源
	registerSafeExit()
	//监听并接收ack，检测程序结束
	listenAndReceive(config.NumOfWorkers)

	GetReqsFromTrace()
	policyID := int32(*curPolicy)
	//curPolicyVal := atomic.LoadInt32(&policyID)
	//for curPolicyVal < config.NumOfAlgorithm {
		start(totalReqs)
		//保证主线程运行
		for  {
			isRoundFinished := atomic.LoadInt32(&roundFinished)
			if isRoundFinished == 1 {
				//进入下一轮
				atomic.AddInt32(&policyID, 1)
				break
			}
		}
		//curPolicyVal = atomic.LoadInt32(&policyID)
	//}
	//清空
	clearAll()
	//通知各个节点退出
	if *closeNodes == 1{
		notifyNodesQuit()
	}

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

func GetReqsFromTrace() []*config.ReqData {

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

	return  totalReqs
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
func syncSettings(policyType int32)  {

	//UsingMulticast := checkMulti(policyType)
	p := &config.Policy{
		Type:      policyType,
		NumOfMB:   *NumOfMB,
		TraceName: *traceName,
	}

	config.NumOfMB = int(*NumOfMB)
	config.BlockSize = int(*NumOfMB * config.MB)

	log.Printf("初始化共享池...%v\n", p)
	config.InitBufferPool()

	for _, ip := range config.NodeIPs{
		common.SendData(p, ip, config.NodeSettingsListenPort)
	}
	log.Printf("等待设置完成...\n")
	time.Sleep(2 * time.Second)

}

func start(reqs []*config.ReqData)  {

	beginTime = time.Now()
	syncSettings(int32(*curPolicy))
	log.Printf(" 设置当前算法：[%s], 当前数据集为：%s, blockSize=%vMB.\n", config.Policies[*curPolicy], OutFilePath, *NumOfMB)

	//重置为本轮未结束：0
	atomic.StoreInt32(&roundFinished, 0)

	log.Printf(" [%s]算法开始运行，总共block请求数量为：%d\n", config.Policies[*curPolicy], numOfReq)
	schedule.SetPolicy(config.Policies[*curPolicy])
	schedule.GetCurPolicy().HandleReq(reqs)
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
