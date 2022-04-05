package schedule

import (
	"EC/common"
	"EC/config"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"
	"sync"
	"time"
)


/*TUpdate:  handle one block + XOR + tree-structured path + batch */
type TUpdate struct {

}
type CMDWaitingList struct {
	sync.RWMutex
	Queue []*config.CMD
}

type Graph struct {
	N   int //顶点数
	M   int //边数
	Arc [][]byte
}
type Task struct {
	Start byte
	End   byte
	SID   int
	BlockID int
}

func (M *CMDWaitingList) pushCMD(cmd *config.CMD)  {
	M.Lock()
	M.Queue = append(M.Queue, cmd)
	M.Unlock()
}
func (M *CMDWaitingList) updateRunnableCMDs(blockID int)  {
	M.Lock()
	for i := 0; i < len(M.Queue); i++ {
		if j := arrays.Contains(M.Queue[i].Helpers, blockID); j >= 0 {
			//M.Queue[i].Helpers[len(M.Queue[i].Helpers)-1], M.Queue[i].Helpers[j] =
			//				M.Queue[i].Helpers[j], M.Queue[i].Helpers[len(M.Queue[i].Helpers)-1]
			//M.Queue[i].Helpers = M.Queue[i].Helpers[:len(M.Queue[i].Helpers)-1]
			//M.Queue[i].Helpers = append(M.Queue[i].Helpers[:j], M.Queue[i].Helpers[j+1:]...)
			M.Queue[i].Matched++
		}
	}

	M.Unlock()
}
func (M *CMDWaitingList) popRunnableCMDs() []*config.CMD  {
	M.Lock()
	cmds := make([]*config.CMD, 0, len(M.Queue))
	for _, cmd := range M.Queue {
		if len(cmd.Helpers) == cmd.Matched {
			cmds = append(cmds, cmd)
		}
	}

	//删除
	for i:= 0; i < len(M.Queue); {
		if len(M.Queue[i].Helpers) == M.Queue[i].Matched {
			M.Queue = append(M.Queue[:i], M.Queue[i+1:]...)
		} else {
			i++
		}
	}

	M.Unlock()
	return cmds
}

func (M *CMDWaitingList) popRunnableCMDsWithSID(sid int) []*config.CMD  {
	M.Lock()
	cmds := make([]*config.CMD, 0, len(M.Queue))
	for _, cmd := range M.Queue {
		if cmd.SID == sid {
			cmds = append(cmds, cmd)
		}
	}

	//删除
	for i:= 0; i < len(M.Queue); {
		if M.Queue[i].SID == sid {
			M.Queue = append(M.Queue[:i], M.Queue[i+1:]...)
		} else {
			i++
		}
	}

	M.Unlock()
	return cmds
}

var CMDList *CMDWaitingList
const MAX_COUNT int = config.M + 1
const INFINITY byte = 255
//var CMDWaitingQueue = make([]*config.CMD, 0, config.MaxBatchSize)
var NodeMatrix = make(config.Matrix, (config.N)*(config.N))
func TaskAdjust(taskGroup []Task)  {
	for _, t := range taskGroup {
		s, e :=  t.Start, t.End
		if s > e {
			t.Start, t.End = t.End, t.Start
		}
	}
}
func (p TUpdate) Init()  {

	InitNetworkDistance()
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	ClearChannels()

	actualBlocks = 0
	sid = 0
	round = 0
	totalCrossRackTraffic = 0
}

func (p TUpdate) HandleReq(reqs []*config.ReqData)  {

	//接收reqs
	actualBlocks = len(reqs)

	//记录ack
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	//处理blocks
	sid = 0
	for _, req := range reqs {
		req := &config.ReqData{
			BlockID: req.BlockID,
			SID:     sid,
		}
		p.handleOneBlock(req)
		sid++
	}
}

func (p TUpdate) handleOneBlock(reqData * config.ReqData)  {
	tasks := GetTransmitTasks(reqData)
	log.Printf("tasks: %v\n", tasks)
	for _, task := range tasks {
		fromIP := common.GetNodeIP(int(task.Start))
		toIPs := []string{common.GetNodeIP(int(task.End))}
		common.SendCMD(fromIP, toIPs, task.SID, task.BlockID)
		//统计跨域流量
		rack1 := getRackIDFromNodeID(task.Start)
		rack2 := getRackIDFromNodeID(task.End)
		if rack1 != rack2 {
			 totalCrossRackTraffic += config.BlockSize
		}
	}
}

func (p TUpdate) HandleTD(td *config.TD)  {

	//本地数据更新
	common.WriteDeltaBlock(td.BlockID, td.Buff)

	//有可以执行的等待任务
	cmds := CMDList.popRunnableCMDsWithSID(td.SID)
	if len(cmds) > 0 {
		//添加ack监听
		for _, cmd := range cmds {
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		for _, cmd := range cmds {

			begin := time.Now()

			for _, toIP := range cmd.ToIPs {

				var SendTD config.TD
				//SendTD := config.TDBufferPool.Get().(*config.TD)
				SendTD.BlockID = cmd.BlockID
				SendTD.Buff = make([]byte, cmd.SendSize)
				SendTD.Buff = td.Buff[:cmd.SendSize]
				SendTD.FromIP = cmd.FromIP
				SendTD.ToIP = toIP
				SendTD.SID = cmd.SID
				SendTD.SendSize = cmd.SendSize

				sendSizeRate := float32(SendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
				log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", SendTD.BlockID, sendSizeRate, toIP)

				common.SendData(SendTD, toIP, config.NodeTDListenPort)

			}

			elapsed := time.Since(begin)
			log.Printf("发送 block %d 给 %v， 发送大小为：%vMB， 用时：%s.\n", cmd.BlockID, cmd.ToIPs, len(td.Buff),
			elapsed)

		}

	//没有可执行的等待任务（叶子节点）
	}else{
		if _, ok := ackMaps.getACK(td.SID); !ok {
			//返回ack
			ack := &config.ACK{
				SID:     td.SID,
				BlockID: td.BlockID,
			}
			ReturnACK(ack)
		}
	}
}

func InitNetworkDistance()  {
	for i := 0; i < config.N; i++ {
		for j := 0; j < config.N; j++ {
			if i == j {
				NodeMatrix[i*config.N+j] = 0
			}else{
				NodeMatrix[i*config.N+j] = 2
			}
		}
	}
	//初始化Rack内部网络距离
	for r := 0; r < config.N / config.RackSize; r++ {
		curRackMinNode := config.RackSize * r
		curRackMaxNode := config.RackSize * r + config.RackSize
		for i := curRackMinNode; i < curRackMaxNode; i++ {
			for j := curRackMinNode; j < curRackMaxNode; j++ {
				if i != j {
					NodeMatrix[i*config.N+j] = 1
				}
			}
		}
	}
}
/*Prim算法*/
func Prim(G Graph) config.Matrix{
	var lowCost = [MAX_COUNT]byte{}
	var vertex = make(config.Matrix, MAX_COUNT)
	lowCost[0] = 0
	for j := 1; j < G.N; j++ {
		lowCost[j] = G.Arc[0][j]
		vertex[j] = 0
	}
	for i := 1; i < G.N; i++ {
		k := 1
		min := INFINITY
		for j := 1; j < G.N; j++ {
			if lowCost[j] != 0 && lowCost[j] < min {
				min = lowCost[j]
				k = j
			}
		}
		lowCost[k] = 0
		for j := 0; j < G.N; j++ {
			if lowCost[j] != 0 && G.Arc[k][j] < lowCost[j] {
				lowCost[j] = G.Arc[k][j]
				vertex[j] = byte(k)
			}
		}
	}
	return vertex
}
/*构造函数*/
func NewGraph(N int) Graph {
	buf := make([][]byte, N)
	for i := 0; i < N; i++ {
		buf[i] = make([]byte, N)
	}
	return Graph{
		N: N,
		M: 0,
		Arc: buf,
	}
}
/*测试*/
func GetMSTPath(matrix, nodeIndexs config.Matrix) config.Matrix   {
	len := len(nodeIndexs)
	G := NewGraph(len)
	for i := 0; i < G.N; i++ {
		for j := 0; j < G.N; j++ {
			G.Arc[i][j] = matrix[i*len+j]
		}
	}
	path := Prim(G)
	return path
}

func getBalancePath(path, nodeIndexes []byte) []byte  {
	childNum := initChild(path)

	for i := 0; i < len(nodeIndexes); i++ {
		for childNum[i] > 2 {
			j := findOne(i, path)
			adjustOnce(j, path, childNum)
		}
	}
	return path
}

func initChild(path []byte) []int {
	childNum := make([]int, len(path))
	for i, v := range path {
		//root节点
		if i == int(v) {
			continue
		}
		childNum[v]++
	}
	return childNum
}

func findOne(i int, path []byte) int {
	for j, v := range path{
		if int(v) == i {
			return j
		}
	}
	return -1
}

func adjustOnce(i int, path []byte, childNum []int) {
	originNode := i
	//前向查找
	p := i - 1
	for p >= 0 {
		if childNum[p] < 2 {
			childNum[path[originNode]]--
			path[i] = byte(p)
			childNum[p]++
			return
		}
		p--
	}
	p = i + 1
	for p < len(childNum) {
		if childNum[p] < 2 {
			path[i] = byte(p)
			childNum[p]++
			childNum[originNode]--
			return
		}
		p++
	}
}

func GetTransmitTasks(reqData *config.ReqData) []Task {
	parities :=	common.RelatedParities(reqData.BlockID)
	parityNodes := common.RelatedParityNodes(parities)
	nodeID := common.GetNodeID(reqData.BlockID)
	relatedParityMatrix, nodeIndexs := getAdjacentMatrix(parityNodes, nodeID, NodeMatrix)
	path := GetMSTPath(relatedParityMatrix, nodeIndexs)

	taskGroup := make([]Task, 0, len(nodeIndexs)-1)
	for i := 1; i < len(nodeIndexs); i++ {
		taskGroup = append(taskGroup, Task{Start: nodeIndexs[path[i]], SID: reqData.SID, BlockID: reqData.BlockID, End:nodeIndexs[i]})
	}
	TaskAdjust(taskGroup)
	sort.SliceStable(taskGroup, func(i, j int) bool {
		return taskGroup[i].Start < taskGroup[j].Start
	})
	return taskGroup
}
func getAdjacentMatrix(parities []byte, nodeID int, allMatrix []byte) (config.Matrix, config.Matrix) {
	nodeIDs := make(config.Matrix, 0, (1+config.M)*(1+config.M))
	nodeIDs = append(nodeIDs, (byte)(nodeID))
	for i := 0; i < len(parities); i++ {
		if arrays.Contains(nodeIDs, parities[i]) < 0 {
			nodeIDs = append(nodeIDs, parities[i])
		}
	}
	len := len(nodeIDs) //[0 4 5]
	newMatrix := make(config.Matrix, len*len)
	for i := 0; i < len; i++ {
		for j := 0; j < len; j++ {
			value :=  nodeIDs[i]*(byte)(config.N)+ nodeIDs[j]
			newMatrix[i*len+j] = allMatrix[value]
		}
	}
	return newMatrix, nodeIDs
}
func (p TUpdate) HandleCMD(cmd *config.CMD)  {

	//helpers已就位
	if IsCMDDataExist(cmd) {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}

		//读取数据
		buff := common.ReadBlockWithSize(cmd.BlockID, config.BlockSize)

		//记录发送数据起始时间
		begin := time.Now()

		//发送数据
		for _, toIP := range cmd.ToIPs {
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff: buff,
				FromIP: cmd.FromIP,
				ToIP: toIP,
				SID: cmd.SID,
				SendSize: cmd.SendSize,
			}
			common.SendData(td, toIP, config.NodeTDListenPort)
		}

		//记录发送数据耗时
		elapsed := time.Since(begin)
		log.Printf("发送 block %d 给 %v 用时：%s.\n", cmd.BlockID, cmd.ToIPs, elapsed)

		//内存回收
		config.BlockBufferPool.Put(buff)

	//helpers未就位
	}else{
		cmd.Helpers = append(cmd.Helpers, cmd.BlockID)
		log.Printf("添加sid: %d, blockID: %d, helpers: %v到cmdList.\n", cmd.SID, cmd.BlockID, cmd.Helpers)
		CMDList.pushCMD(cmd)
	}
}
func meetCMDNeed(blockID int) []*config.CMD  {
	CMDList.updateRunnableCMDs(blockID)
	return CMDList.popRunnableCMDs()
}
func IsCMDDataExist(cmd *config.CMD) bool {
	return common.GetNodeIP(common.GetNodeID(cmd.BlockID)) == common.GetLocalIP()
}

func (p TUpdate) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			IsRunning = false
		}
	}
}
func (p TUpdate) Clear()  {
	sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
	actualBlocks = 0
}

func (p TUpdate) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p TUpdate) IsFinished() bool {
	return ackMaps.isEmpty()
}


func (p TUpdate) GetActualBlocks() int {
	return actualBlocks
}

func (p TUpdate) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.Megabyte
}