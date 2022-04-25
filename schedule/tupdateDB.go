package schedule

import (
	"EC/common"
	"EC/config"
	"log"
)
/*TUpdate:  delta + handle one block + XOR + tree-structured path + batch */

type TUpdateDB struct {

}

func (p TUpdateDB) Init()  {

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

	totalCrossRackTraffic = 0
	actualBlocks = 0
	round = 0
	sid = 0

	ClearChannels()
}

func (p TUpdateDB) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		lenOfBatch := findDistinctReqs()
		actualBlocks += len(curDistinctReq)
		log.Printf("第%d轮 TUpdateDB：获取%d个请求，实际处理%d个block\n", round, lenOfBatch, len(curDistinctReq))

		//执行reqs
		p.TUpdateDB(curDistinctReq)

		select {
		case <-Done:
			log.Printf("本轮结束！\n")
			log.Printf("======================================\n")
			round++
			p.Clear()
		}
	}
}

func (p TUpdateDB) TUpdateDB(reqs []*config.ReqData)   {

	oldSid := sid
	//记录ack
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	sid = oldSid
	for _, req := range reqs {
		req.SID = sid
		p.handleOneReq(req)
		sid++
	}
}





func (p TUpdateDB) handleOneReq(reqData * config.ReqData)  {
	tasks := GetBalanceTransmitTasks(reqData)
	//tasks := GetTransmitTasks(reqData)
	log.Printf("tasks: %v\n", tasks)
	for _, task := range tasks {

		//构造cmd
		fromIP := common.GetNodeIP(int(task.Start))
		toIPs := []string{common.GetNodeIP(int(task.End))}
		SendSize := reqData.RangeRight - reqData.RangeLeft
		helpers := make([]int, 0, 1)
		sendSizeKB := float32(SendSize) / 1024.0 //转为KB
		log.Printf("task %d: send block %d with size %fKB from %s to %v.\n", task.SID, task.BlockID, sendSizeKB, fromIP, toIPs)

		common.SendCMDWithSizeAndHelper(fromIP, toIPs, task.SID, task.BlockID, SendSize, helpers)

		//统计跨域流量
		rack1 := getRackIDFromNodeID(task.Start)
		rack2 := getRackIDFromNodeID(task.End)
		if rack1 != rack2 {
			totalCrossRackTraffic += SendSize
		}
	}
}

func (p TUpdateDB) HandleTD(td *config.TD)  {

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
			for _, toIP := range cmd.ToIPs {
				SendTD := &config.TD{
					BlockID: cmd.BlockID,
					Buff: td.Buff,
					FromIP: cmd.FromIP,
					ToIP: toIP,
					SID: cmd.SID,
					SendSize: cmd.SendSize,
				}
				//sendSizeRate := float32(SendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
				//log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", SendTD.BlockID, sendSizeRate, toIP)
				//common.SendData(SendTD, toIP, config.NodeTDListenPort, "")

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

func (p TUpdateDB) HandleCMD(cmd *config.CMD)  {

	//helpers已到位
	if IsCMDDataExist(cmd) {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)

		for _, toIP := range cmd.ToIPs {
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff: buff,
				FromIP: cmd.FromIP,
				ToIP: toIP,
				SID: cmd.SID,
			}
			//td := config.TDBufferPool.Get().(*config.TD)
			//td.BlockID = cmd.BlockID
			//td.Buff = buff
			//td.FromIP = cmd.FromIP
			//td.ToIP = toIP
			//td.SID = cmd.SID
			common.SendData(td, toIP, config.NodeTDListenPort)

			//config.TDBufferPool.Put(td)
		}
		config.BlockBufferPool.Put(buff)

	//helpers未到位
	}else{
		cmd.Helpers = append(cmd.Helpers, cmd.BlockID)
		log.Printf("添加sid: %d, blockID: %d, helpers: %v到cmdList.\n", cmd.SID, cmd.BlockID, cmd.Helpers)
		CMDList.pushCMD(cmd)
	}
}
func (p TUpdateDB) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		//检查是否全部完成，若完成，进入下一轮
		}else if ACKIsEmpty() {
			Done <- true
		}
	}
}
func (p TUpdateDB) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}
func (p TUpdateDB) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdateDB) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p TUpdateDB) GetActualBlocks() int {
	return actualBlocks
}

func (p TUpdateDB) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.Megabyte
}