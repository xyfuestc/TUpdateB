package schedule

import (
	"EC/common"
	"EC/config"
	"log"
)

// PDN_P delta传输
type PDN_P struct {

}

func (p PDN_P) HandleCMD(cmd *config.CMD) {
	handleOneCMD(cmd)
}

func (p PDN_P) HandleTD(td *config.TD)  {
	handleOneTD(td)
}
func (p PDN_P) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		////ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			Done <- true
		}
	}
}


func (p PDN_P) Init()  {
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	actualBlocks = 0
	round = 0
	totalCrossRackTraffic = 0
	ClearChan()
	sid = 0
}



func (p PDN_P) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs

	for len(totalReqs) > 0 {
		//过滤blocks
		batchReqs := getBatchReqs()
		actualBlocks += len(batchReqs)
		log.Printf("第%d轮 PDN-P 处理%d个block\n", round, len(batchReqs))
		//执行base
		p.pdn_p(batchReqs)

		//select {
		//case <-Done:
		//	log.Printf("本轮结束！\n")
		//	log.Printf("======================================\n")
			round++
			p.Clear()
		//}
	}


}
func GetSourceNodeNums(reqs []*config.ReqData) map[int]int {
	nodes := map[int]int{}
	for _, req := range reqs {
		 nodeID := common.GetNodeID(req.BlockID)
		if _, ok := nodes[nodeID]; ok {
			// 存在
			nodes[nodeID] += 1
		}else{
			nodes[nodeID] = 1
		}
	}
	return nodes
}
func (p PDN_P) pdn_p(reqs []*config.ReqData)  {
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	sid = 0
	//nodes := GetSourceNodeNums(reqs)
	//fmt.Println(nodes)
	for _, req := range reqs {
		req.SID = sid
		p.handleOneBlock(*req)
		sid++
	}
}
func (p PDN_P) handleOneBlock(reqData config.ReqData)  {
	nodeID := common.GetNodeID(reqData.BlockID)
	fromIP := common.GetNodeIP(nodeID)
	toIPs := common.GetRelatedParityIPs(reqData.BlockID)
	common.SendCMDWithSizeAndHelper(fromIP, toIPs, reqData.SID, reqData.BlockID,
		reqData.RangeRight-reqData.RangeLeft, nil)
	//跨域流量统计
	totalCrossRackTraffic += len(toIPs) * (reqData.RangeRight - reqData.RangeLeft)
	log.Printf("(%v, %v, %v)，size：%v KB\n", nodeID, reqData.BlockID, toIPs, float32(reqData.RangeRight - reqData.RangeLeft)/config.KB)
}
func (p PDN_P) RecordSIDAndReceiverIP(sid int, ip string)  {
	ackIPMaps.recordIP(sid, ip)
}
func (p PDN_P) Clear()  {
	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	IsRunning = true
}


func (p PDN_P) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}

func (p PDN_P) GetActualBlocks() int {
	return actualBlocks
}
//数据格式：MB
func (p PDN_P) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.MB
}

