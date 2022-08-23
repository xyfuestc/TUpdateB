package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"bufio"
	"flag"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	testNum     = 5
)
var num = 0
var OutFilePath = ""
var SpaceFilePath = "../log/space_log.txt"
var actNum = 0
var beginTime time.Time
var totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
var roundFinished int32 = 0  // 1-本轮结束 ； 0-本轮未结束
var Done = make(chan bool, 1)

var t time.Duration = 0
var NumOfMB = flag.Float64("b", 4, "块大小：int型参数，默认：64MB")
var policyID = flag.Int("p", 0, "策略ID：0-Base;1-CRRepairBoost;2-Express，默认：0")
var closeNodes = flag.Int("c", 0, "是否程序结束自动关闭各节点？1-关闭；0-不关闭，默认：0")
var traceName = flag.String("f", "rsrch_2", "日志文件名，默认：rsrch_2")
var throughputs = make([]float64, 0, testNum)
var traffics = make([]float64, 0, testNum)

func checkFinish() {

	RoundFinished := atomic.LoadInt32(&roundFinished)
	p := int32(*policyID)
	var policy = atomic.LoadInt32(&p)
	if RoundFinished == 0 && schedule.GetPolicy().IsFinished() && policy < config.NumOfAlgorithm {

		//清空ACK
		schedule.ClearChan()

		t = time.Since(beginTime)


		throughput :=  float64(num) * float64(*NumOfMB) / t.Seconds()
		//throughputs = append(throughputs, throughput)
		actNum = schedule.GetPolicy().GetActualBlocks()
		speed := float64(t/time.Millisecond) / float64(actNum) / 1000       //单块更新时间（s）
		traffic := float64(schedule.GetPolicy().GetCrossRackTraffic()) / float64(num)
		//traffics = append(traffics, traffic)

		log.Printf("%s 总耗时: %.2fs, 完成更新任务: %d, 实际处理任务数: %d, 单块更新时间: %0.2fs, 吞吐量: %0.2fMB/s，单块平均跨域流量为：%0.3fMB\n",
			config.Policies[policy], t.Seconds(), num, actNum, speed, throughput, traffic)

		//本轮结束
		atomic.StoreInt32(&roundFinished, 1)

		schedule.GetPolicy().Clear()
		clear()
		//表明本算法结束
		Done <- true
	}
}
/*所有算法跑完，清空操作*/
func clearAll() {
	log.Printf("清空所有数据和资源...\n")
	actNum = 0
	num = 0
}
/*每种算法结束后，清空操作*/
func clear()  {
	actNum = 0
}
func main() {
	//defer profile.Start(profile.MemProfile, profile.MemProfileRate(1)).Stop()

	flag.Parse()
	OutFilePath = "../request/"+*traceName+"_"+strconv.Itoa(int(*NumOfMB))+"M.csv.txt"

	config.Init()
	log.Printf("ms启动...监听ack: %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)

	registerSafeExit()                          //当发生意外退出时，释放所有资源
	listenAndReceive(config.NumOfWorkers)       //监听并接收ack，检测程序结束

	GetReqsFromTrace()                          //获取访问记录
	//policyValue := int32(*policyID)
	//curPolicyVal := atomic.LoadInt32(&policyValue)
	//for i := 1; i <= testNum; i++ {
	//	log.Printf("算法:%v 第%v次实验.", config.Policies[policyValue], i)
		clear()
		start(totalReqs)
		//保证主线程运行
		for  {
			isRoundFinished := atomic.LoadInt32(&roundFinished)
			if isRoundFinished == 1 {
				//进入下一轮
				break
			}
		}
	//}

	time.Sleep(1 * time.Second)     //万一有ack返回来，等一会再结束
	//清空
	clearAll()
	//通知各个节点退出
	if *closeNodes == 1{
		notifyNodesQuit()
	}
	//throughputMin, throughputMax, throughputAver := getMinMaxAver(throughputs)
	//trafficMin, trafficMax, trafficAver := getMinMaxAver(traffics)
	//fmt.Printf("运行%v次算法【%v】结束...吞吐量:[%v, %v], 平均值：%v, 跨域流量:[%v, %v], 平均值：%v \n",
	//				testNum, config.Policies[policyValue], throughputMin, throughputMax, throughputAver, trafficMin, trafficMax, trafficAver)

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
	num = len(totalReqs)

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
	time.Sleep(2 * time.Second)
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
	syncSettings(int32(*policyID))
	log.Printf(" 设置当前算法：[%s], 当前数据集为：%s, blockSize=%vMB.\n", config.Policies[*policyID], OutFilePath, *NumOfMB)

	//重置为本轮未结束：0
	atomic.StoreInt32(&roundFinished, 0)

	log.Printf(" [%s]算法开始运行，总共block请求数量为：%d\n", config.Policies[*policyID], num)
	schedule.SetPolicy(config.Policies[*policyID])
	schedule.GetPolicy().HandleReq(reqs)
}
func msgSorter(receivedAckCh <-chan config.ACK)  {
	for  {
		select {
		case ack := <- receivedAckCh:
			schedule.GetPolicy().HandleACK(&ack)
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
			schedule.GetPolicy().Clear()
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

func getMinMaxAver(results []float64) (min, max, aver float64) {
	sum := 0.0
	min = math.MaxFloat64
	max = -1
	for _, result := range results {
		sum += result
		if result < min {
			min = result
		}
		if result > max {
			max = result
		}
	}
	aver = sum / float64(len(results))

	return min, max, aver
}
