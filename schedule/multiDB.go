package schedule

import (
	"EC/common"
	"EC/config"
	"log"
)

/*MultiDB: delta + handle one block + XOR + star-structured + multicast + batch */
type MultiDB struct {

}

func (p MultiDB) HandleCMD(cmd *config.CMD) {
	//重复SID，不处理
	if _, ok := ackMaps.getACK(cmd.SID); ok {
		return
	}

	//利用多播将数据发出
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID, cmd.SendSize)

	for _, _ = range cmd.ToIPs {
		ackMaps.pushACK(cmd.SID)
	}

	count := 1 + cmd.SendSize / config.MTUSize

	message := &config.MTU{
		BlockID:        cmd.BlockID,
		Data:           buff[:config.MTUSize],
		FromIP:         cmd.FromIP,
		MultiTargetIPs: cmd.ToIPs,
		SID:            cmd.SID,
		FragmentID:     0,
		FragmentCount: 	count,
		IsFragment:     false,
		SendSize:       cmd.SendSize,
	}
	MulticastSendMTUCh <- *message
	SentMsgLog.PushMsg(message.SID, *message) //记录block

	config.BlockBufferPool.Put(buff)
	log.Printf("HandleCMD: 发送td(sid:%d, blockID:%d)，从%s到%v \n", cmd.SID, cmd.BlockID, common.GetLocalIP(), cmd.ToIPs)

}

func (p MultiDB) HandleTD(td *config.TD)  {
	handleOneTD(td)
}
func (p MultiDB) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		SentMsgLog.popMsg(ack.SID)      //该SID重发数量-1
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ack.CrossTraffic = totalCrossRackTraffic
			ack.FromIP = common.GetLocalIP()
			ReturnACK(ack)
			//ms
		}else {
			//统计其他节点重发的流量
			DataNodeResentTraffic[ack.FromIP] = ack.CrossTraffic
			if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
				IsRunning = false
			}
		}
	}
}

func (p MultiDB) Init()  {
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	DataNodeResentTraffic = make(map[string]int)
	actualBlocks = 0
	round = 0
	totalCrossRackTraffic = 0
	sid = 0
	SentMsgLog.Init()
	ClearChan()
}


func (p MultiDB) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs

	for len(totalReqs) > 0 {
		lenOfBatch := findDistinctReqs()
		actualBlocks += len(curDistinctReq)
		log.Printf("第%d轮 MultiDB：获取%d个请求，实际处理%d个block，剩余%v个block待处理。\n", round, lenOfBatch, len(curDistinctReq), len(totalReqs))

		//处理reqs
		p.baseMulti(curDistinctReq)

		for IsRunning {

		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}
}
func (p MultiDB) baseMulti(reqs []*config.ReqData)  {
	oldSIDStart := sid
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	sid = oldSIDStart
	for _, req := range reqs {
		req.SID = sid
		p.handleOneBlock(*req)
		sid++
	}
}
func (p MultiDB) handleOneBlock(reqData config.ReqData)  {
	nodeID := common.GetNodeID(reqData.BlockID)
	fromIP := common.GetNodeIP(nodeID)
	toIPs := common.GetRelatedParityIPs(reqData.BlockID)
	//common.SendCMD(fromIP, toIPs, reqData.SID, reqData.BlockID)

	rangeLeft, rangeRight := reqData.RangeLeft, reqData.RangeRight
	cmd := &config.CMD{
		SID: sid,
		BlockID: reqData.BlockID,
		ToIPs: toIPs,
		FromIP: fromIP,
		Helpers: make([]int, 0, 1),
		Matched: 0,
		SendSize: rangeRight - rangeLeft,
		//SendSize: config.BlockSize,
	}
	common.SendData(cmd, fromIP, config.NodeCMDListenPort)

	//跨域流量统计
	totalCrossRackTraffic += rangeRight - rangeLeft
	log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v. totalCrossRackTraffic: %v\n", reqData.SID,
		nodeID, common.GetNodeIP(nodeID), reqData.BlockID, toIPs, totalCrossRackTraffic)
}
func (p MultiDB) RecordSIDAndReceiverIP(sid int, ip string)  {
	ackIPMaps.recordIP(sid, ip)
}
func (p MultiDB) Clear()  {

	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}

	ClearChan()

	IsRunning = true
	//清空SentMsgLog
	SentMsgLog.Init()

}
func (p MultiDB) IsFinished() bool {
	isFinished :=  len(totalReqs) == 0 && ackMaps.isEmpty()
	if isFinished {
		//CloseAllChannels()
	}
	return isFinished
}

func (p MultiDB) GetActualBlocks() int {
	return actualBlocks
}
func (p MultiDB) GetCrossRackTraffic() float32 {
	for ip, traffic := range DataNodeResentTraffic{
		totalCrossRackTraffic += traffic
		log.Printf("%v's resent traffic is: %v", ip, traffic)
	}
	return  float32(totalCrossRackTraffic) / config.MB
}