package schedule

import (
	"EC/common"
	"EC/config"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"
	"sync"
)
/*TUpdate:  delta + handle one block + XOR + tree-structured path */

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

	totalCrossRackTraffic = 0
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
	actualBlocks = 0
	sid = 0
	round = 0
	ClearChannels()
}

func (p TUpdate) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		curMatchBlocks := FindDistinctBlocks()
		actualBlocks += len(curMatchBlocks)
		log.Printf("第%d轮 TUpdate：获取%d个请求，实际处理%d个block\n", round, len(curMatchBlocks), len(curMatchBlocks))

		//处理reqs
		p.TUpdateD(curMatchBlocks)

		select {
		case <-Done:
			log.Printf("本轮结束！\n")
			log.Printf("======================================\n")
			round++
			p.Clear()
		}
	}
}

func (p TUpdate) TUpdateD(reqs []*config.ReqData)  {
	//记录ack
	oldSID := sid
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	//处理blocks
	sid = oldSID
	for _, req := range reqs {
		req.SID = sid
		p.handleOneBlock(req)
		sid++
	}
}

func (p TUpdate) handleOneBlock(reqData * config.ReqData)  {
	tasks := GetBalanceTransmitTasks(reqData)
	//tasks := GetTransmitTasks(reqData)
	log.Printf("TUpdate Tasks: %v\n", tasks)
	for _, task := range tasks {
		fromIP := common.GetNodeIP(int(task.Start))
		toIPs := []string{common.GetNodeIP(int(task.End))}
		sendSize := reqData.RangeRight - reqData.RangeLeft
		helpers := make([]int, 0, 1)
		common.SendCMDWithSizeAndHelper(fromIP, toIPs, task.SID, task.BlockID, sendSize, helpers)
		sendSizeKB := float32(sendSize) / 1024.0 //转为KB
		log.Printf("task %d: send block %d with size %fKB from %s to %v.\n", task.SID, task.BlockID, sendSizeKB, fromIP, toIPs)
		//统计跨域流量
		rack1 := getRackIDFromNodeID(task.Start)
		rack2 := getRackIDFromNodeID(task.End)
		if rack1 != rack2 {
			totalCrossRackTraffic += reqData.RangeRight - reqData.RangeLeft
		}
	}
}

func (p TUpdate) HandleTD(td *config.TD)  {

	//本地数据更新
	common.WriteDeltaBlock(td.BlockID, td.Buff)

	//有等待任务
	cmds := CMDList.popRunnableCMDsWithSID(td.SID)
	if len(cmds) > 0 {
		//添加ack监听
		for _, cmd := range cmds {
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		for _, cmd := range cmds {
			for _, toIP := range cmd.ToIPs {

				SendTD := &config.TD{
					BlockID: cmd.BlockID,
					Buff: td.Buff[:cmd.SendSize],
					FromIP: cmd.FromIP,
					ToIP: toIP,
					SID: cmd.SID,
					SendSize: cmd.SendSize,
				}
				//
				//SendTD := config.TDBufferPool.Get().(*config.TD)
				//SendTD.BlockID = cmd.BlockID
				//SendTD.Buff = td.Buff[:cmd.SendSize]
				//SendTD.FromIP = cmd.FromIP
				//SendTD.ToIP = toIP
				//SendTD.SID = cmd.SID
				//SendTD.SendSize = cmd.SendSize
				sendSizeRate := float32(SendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
				log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", SendTD.BlockID, sendSizeRate, toIP)
				common.SendData(SendTD, toIP, config.NodeTDListenPort)


				//config.TDBufferPool.Put(SendTD)
			}
		}
		//叶子节点
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
func meetCMDNeed(blockID int) []*config.CMD  {
	CMDList.updateRunnableCMDs(blockID)
	return CMDList.popRunnableCMDs()
}
func IsCMDDataExist(cmd *config.CMD) bool {
	return common.GetNodeIP(common.GetNodeID(cmd.BlockID)) == common.GetLocalIP()
}
func (p TUpdate) HandleCMD(cmd *config.CMD)  {
	//该cmd的数据存在
	if IsCMDDataExist(cmd) {

		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}

		//读取数据
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)

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

		//内存回收
		config.BlockBufferPool.Put(buff)

		//该cmd的数据不存在（需要等待）
	}else{
		cmd.Helpers = append(cmd.Helpers, cmd.BlockID)
		log.Printf("添加sid: %d, blockID: %d, helpers: %v到cmdList.\n", cmd.SID, cmd.BlockID, cmd.Helpers)
		CMDList.pushCMD(cmd)
	}
}
func (p TUpdate) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			Done <- true
		}
	}
}
func (p TUpdate) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	//sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}
func (p TUpdate) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdate) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p TUpdate) GetActualBlocks() int {
	return actualBlocks
}

func (p TUpdate) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.MB
}

func  GetBalanceTransmitTasks(reqData *config.ReqData) []Task {
	parities :=	common.RelatedParities(reqData.BlockID)
	parityNodes := common.RelatedParityNodes(parities)
	nodeID := common.GetNodeID(reqData.BlockID)
	relatedParityMatrix, nodeIndexs := getAdjacentMatrix(parityNodes, nodeID, NodeMatrix)
	path := GetMSTPath(relatedParityMatrix, nodeIndexs)

	//bPath := getBalancePath(path, nodeIndexs)
	//log.Printf("bPath : %v\n", bPath)

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
