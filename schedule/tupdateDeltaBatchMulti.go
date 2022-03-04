package schedule

import (
	"EC/common"
	"EC/config"
	"log"
)

type TUpdateDeltaBatchMulti struct {

}

func (p TUpdateDeltaBatchMulti) Init()  {
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
	round = 0
}

func (p TUpdateDeltaBatchMulti) HandleReq(reqs []*config.ReqData)  {
	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		findDistinctReqs()
		actualBlocks += len(curDistinctReq)
		//log.Printf("第%d轮 TUpdateDeltaBatchMulti：处理%d个block\n", round, len(curDistinctBlocks))
		log.Printf("第%d轮 TUpdateDeltaBatchMulti：获取%d个请求，实际处理%d个block\n", round, len(curDistinctReq), len(curDistinctBlocks))

		//执行basex
		p.tupdateDeltaBatch(curDistinctReq)

		for IsRunning {

		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}
}

func (p TUpdateDeltaBatchMulti) tupdateDeltaBatch(reqs []*config.ReqData)   {
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	sid = 0
	for _, req := range reqs {
		p.handleOneBlock(req)
		sid++
	}
}

func (p TUpdateDeltaBatchMulti) handleOneBlock(reqData * config.ReqData)  {
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

func (p TUpdateDeltaBatchMulti) HandleTD(td *config.TD)  {
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

func (p TUpdateDeltaBatchMulti) HandleCMD(cmd *config.CMD)  {
	if IsCMDDataExist(cmd) {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		//log.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlockWithSize(cmd.BlockID, config.BlockSize)

		for _, toIP := range cmd.ToIPs {
			//td := &config.TD{
			//	BlockID: cmd.BlockID,
			//	Buff: buff,
			//	FromIP: cmd.FromIP,
			//	ToIP: toIP,
			//	SID: cmd.SID,
			//}
			td := config.TDBufferPool.Get().(*config.TD)
			td.BlockID = cmd.BlockID
			td.Buff = buff[:config.BlockSize]
			td.FromIP = cmd.FromIP
			td.ToIP = toIP
			td.SID = cmd.SID
			common.SendData(td, toIP, config.NodeTDListenPort)

			config.TDBufferPool.Put(td)
		}
		config.BlockBufferPool.Put(buff)

	}else{
		cmd.Helpers = append(cmd.Helpers, cmd.BlockID)
		log.Printf("添加sid: %d, blockID: %d, helpers: %v到cmdList.\n", cmd.SID, cmd.BlockID, cmd.Helpers)
		CMDList.pushCMD(cmd)
	}
}
func (p TUpdateDeltaBatchMulti) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			IsRunning = false
		}

	}
}
func (p TUpdateDeltaBatchMulti) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}
func (p TUpdateDeltaBatchMulti) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdateDeltaBatchMulti) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p TUpdateDeltaBatchMulti) GetActualBlocks() int {
	return actualBlocks
}