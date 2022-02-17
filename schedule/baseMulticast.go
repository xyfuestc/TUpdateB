package schedule

import (
	"EC/common"
	"EC/config"
	"log"
	"time"
)

/*BaseMulticast: delta + handle one block + XOR + star-structured + multicast */
type BaseMulticast struct {

}
var SendCh = make(chan config.MTU, 10)
var ReceiveCh = make(chan config.MTU, 10)
func (p BaseMulticast) HandleCMD(cmd *config.CMD) {
	//利用多播将数据发出
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID)

	//buff := common.ReadBlock(cmd.BlockID)
	log.Printf("读取到数据 block %d: %v\n", cmd.BlockID, len(buff))
	for _, _ = range cmd.ToIPs {
		ackMaps.pushACK(cmd.SID)
	}
	//2.发送数据
	//count :=  len(buff) / config.MTUSize
	//var sendData []byte

	//if count > 0 {  //分片发送数据
	//	for index := 0; index < count+1; index++ {
	//		length := 0
	//		if index == count { // 处理最后一个分片
	//			length = len(buff) - index*config.MTUSize
	//		} else {
	//			length = config.MTUSize
	//		}
	//		//如果刚好除尽，最后不用处理
	//		if length == 0 {
	//			break
	//		}
	//		sendData = buff[index*config.MTUSize : index*config.MTUSize+length]
	//		message := &config.MTU{
	//			BlockID: cmd.BlockID,
	//			Data: sendData,
	//			FromIP: cmd.FromIP,
	//			MultiTargetIPs: cmd.ToIPs,
	//			SID: cmd.SID,
	//			IsFragment: true,
	//			FragmentID: index,
	//			FragmentCount: count,
	//		}
	//		SendCh <- *message
	//		time.Sleep(2 * time.Second)
	//		//log.Printf("发送sid: %v的第%v（共%d）个分片数据.", cmd.SID, index, count)
	//	}
	//}
	//else{  //数据量小，不需要分片
		message := &config.MTU{
			BlockID: cmd.BlockID,
			Data: buff[:cmd.SendSize],
			FromIP: cmd.FromIP,
			MultiTargetIPs: cmd.ToIPs,
			SID: cmd.SID,
			IsFragment: false,
		}
		SendCh <- *message
		time.Sleep(500 * time.Millisecond)

	//}
	log.Printf("HandleCMD: 发送td(sid:%d, blockID:%d)，从%s到%v \n", cmd.SID, cmd.BlockID, common.GetLocalIP(), cmd.ToIPs)

}
func (p BaseMulticast) HandleTD(td *config.TD)  {
	handleOneTD(td)
}
func (p BaseMulticast) HandleACK(ack *config.ACK)  {
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

func (p BaseMulticast) Init()  {
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	actualBlocks = 0
	round = 0
	totalCrossRackTraffic = 0
}


func (p BaseMulticast) HandleReq(reqs []*config.ReqData)  {
	//actualBlocks = len(reqs)
	totalReqs = reqs

	for len(totalReqs) > 0 {
		//过滤blocks
		////findDistinctReqs()
		//log.Printf("第%d轮 BaseMulticast：处理%d个block\n", round, len(curDistinctReq))
		////执行base
		//p.base(curDistinctReq)
		batchReqs := getBatchReqs()
		actualBlocks += len(batchReqs)
		log.Printf("第%d轮 BaseMulticast：处理%d个block\n", round, len(batchReqs))
		//执行base
		p.base(batchReqs)

		for IsRunning {

		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}


}
func (p BaseMulticast) base(reqs []*config.ReqData)  {
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	sid = 0
	for _, req := range reqs {
		req := config.ReqData{
			BlockID: req.BlockID,
			SID:     sid,
		}
		p.handleOneBlock(req)
		sid++
	}
}
func (p BaseMulticast) handleOneBlock(reqData config.ReqData)  {
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
	common.SendData(cmd, fromIP, config.NodeCMDListenPort, "")

	//跨域流量统计
	totalCrossRackTraffic += len(toIPs) * (rangeRight - rangeLeft)
	log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", reqData.SID,
		nodeID, common.GetNodeIP(nodeID), reqData.BlockID, toIPs)
}
func (p BaseMulticast) RecordSIDAndReceiverIP(sid int, ip string)  {
	ackIPMaps.recordIP(sid, ip)
}
func (p BaseMulticast) Clear()  {
	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	IsRunning = true
}
func (p BaseMulticast) IsFinished() bool {
	isFinished :=  len(totalReqs) == 0 && ackMaps.isEmpty()
	if isFinished {
		close(SendCh)
		close(ReceiveCh)
	}
	return isFinished
}

func (p BaseMulticast) GetActualBlocks() int {
	return actualBlocks
}
func Hit(sid int) bool {
	if _, ok := ackIPMaps.getIP(sid); ok{
		return true
	}else{
		return false
	}
}