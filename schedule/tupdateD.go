package schedule

import (
	"EC/common"
	"EC/config"
	"log"
)
/*TUpdate:  delta + handle one block + XOR + tree-structured path */

type TUpdateD struct {

}

func (p TUpdateD) Init()  {

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

func (p TUpdateD) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		curMatchBlocks := findDistinctBlocks()
		actualBlocks += len(curMatchBlocks)
		log.Printf("第%d轮 TUpdateD：获取%d个请求，实际处理%d个block\n", round, len(curMatchBlocks), len(curMatchBlocks))

		//处理reqs
		p.TUpdateD(curMatchBlocks)

		for IsRunning {
		}

		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++

		p.Clear()
	}
}

func (p TUpdateD) TUpdateD(reqs []*config.ReqData)  {
	//记录ack
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	//处理blocks
	sid = 0
	for _, req := range reqs {
		req.SID = sid
		p.handleOneBlock(req)
		sid++
	}
}

func (p TUpdateD) handleOneBlock(reqData * config.ReqData)  {
	tasks := GetBalanceTransmitTasks(reqData)
	//tasks := GetTransmitTasks(reqData)
	log.Printf("TUpdateD Tasks: %v\n", tasks)
	for _, task := range tasks {
		fromIP := common.GetNodeIP(int(task.Start))
		toIPs := []string{common.GetNodeIP(int(task.End))}
		sendSize := reqData.RangeRight - reqData.RangeLeft
		helpers := make([]int, 0, 1)
		common.SendCMDWithSizeAndHelper(fromIP, toIPs, task.SID, task.BlockID, sendSize, helpers)

		//统计跨域流量
		rack1 := getRackIDFromNodeID(task.Start)
		rack2 := getRackIDFromNodeID(task.End)
		if rack1 != rack2 {
			totalCrossRackTraffic += reqData.RangeRight - reqData.RangeLeft
		}
	}
}

func (p TUpdateD) HandleTD(td *config.TD)  {

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
func (p TUpdateD) HandleCMD(cmd *config.CMD)  {
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
func (p TUpdateD) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			IsRunning = false
			p.Clear()
		}
	}
}
func (p TUpdateD) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}
func (p TUpdateD) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdateD) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p TUpdateD) GetActualBlocks() int {
	return actualBlocks
}

func (p TUpdateD) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.Megabyte
}