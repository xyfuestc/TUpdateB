package schedule

import (
	"EC/common"
	"EC/config"
	"log"
	"sort"
)

type TUpdateBatch struct {

}

func (p TUpdateBatch) Init()  {

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

func (p TUpdateBatch) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		curMatchBlocks := findDistinctBlocks()
		actualBlocks += len(curDistinctBlocks)
		log.Printf("第%d轮 TUpdateBatch：获取%d个请求，实际处理%d个block\n", round, len(curMatchBlocks), len(curDistinctBlocks))

		//处理reqs
		p.TUpdateBatch(curDistinctBlocks)

		for IsRunning {
		}

		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++

		p.Clear()
	}
}

func (p TUpdateBatch) TUpdateBatch(distinctBlocks []int)  {
	//记录ack
	for _, _ = range distinctBlocks {
		ackMaps.pushACK(sid)
		sid++
	}
	//处理blocks
	sid = 0
	for _, blockID := range distinctBlocks {
		req := &config.ReqData{
			BlockID: blockID,
			SID:     sid,
		}
		p.handleOneBlock(req)
		sid++
	}
}

func (p TUpdateBatch) handleOneBlock(reqData * config.ReqData)  {
	tasks := GetBalanceTransmitTasks(reqData)
	//tasks := GetTransmitTasks(reqData)
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

func (p TUpdateBatch) HandleTD(td *config.TD)  {

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
				//SendTD := &config.TD{
				//	BlockID: cmd.BlockID,
				//	Buff: td.Buff,
				//	FromIP: cmd.FromIP,
				//	ToIP: toIP,
				//	SID: cmd.SID,
				//}
				//sendSizeRate := float32(SendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
				//log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", SendTD.BlockID, sendSizeRate, toIP)
				//common.SendData(SendTD, toIP, config.NodeTDListenPort, "")

				SendTD := config.TDBufferPool.Get().(*config.TD)
				SendTD.BlockID = cmd.BlockID
				SendTD.Buff = td.Buff[:cmd.SendSize]
				SendTD.FromIP = cmd.FromIP
				SendTD.ToIP = toIP
				SendTD.SID = cmd.SID
				SendTD.SendSize = cmd.SendSize
				sendSizeRate := float32(SendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
				log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", SendTD.BlockID, sendSizeRate, toIP)
				common.SendData(SendTD, toIP, config.NodeTDListenPort)

				config.TDBufferPool.Put(SendTD)
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
func  GetBalanceTransmitTasks(reqData *config.ReqData) []Task {
	parities :=	common.RelatedParities(reqData.BlockID)
	parityNodes := common.RelatedParityNodes(parities)
	nodeID := common.GetNodeID(reqData.BlockID)
	relatedParityMatrix, nodeIndexs := getAdjacentMatrix(parityNodes, nodeID, NodeMatrix)
	path := GetMSTPath(relatedParityMatrix, nodeIndexs)

	bPath := getBalancePath(path, nodeIndexs)
	log.Printf("bPath : %v\n", bPath)

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
func (p TUpdateBatch) HandleCMD(cmd *config.CMD)  {
	//该cmd的数据存在
	if IsCMDDataExist(cmd) {

		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}

		//读取数据
		buff := common.ReadBlockWithSize(cmd.BlockID, config.BlockSize)

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
func (p TUpdateBatch) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//SentMsgLog.popMsg(ack.SID)      //该SID不需要重发
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			IsRunning = false
		}
	}
}
func (p TUpdateBatch) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}
func (p TUpdateBatch) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdateBatch) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p TUpdateBatch) GetActualBlocks() int {
	return actualBlocks
}