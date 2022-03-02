package schedule

import (
	"EC/common"
	"EC/config"
	"log"
	"sync"
	"time"
)

type Policy interface {
	Init()
	HandleReq(reqs []*config.ReqData)
	HandleCMD(cmd *config.CMD)
	HandleTD(td *config.TD)
	HandleACK(ack *config.ACK)
	Clear()
	RecordSIDAndReceiverIP(sid int, ip string)
	IsFinished() bool
	GetActualBlocks() int
}

type Base struct {

}

type ACKMap struct {
	sync.RWMutex
	RequireACKs map[int]int
}
func (M *ACKMap) getACK(sid int) (int, bool)  {
	M.RLock()
	num, ok := M.RequireACKs[sid]
	M.RUnlock()
	return num, ok
}
func (M *ACKMap) pushACK(sid int)  {
	M.Lock()
	if _, ok := M.RequireACKs[sid]; !ok {
		M.RequireACKs[sid] = 1
	}else{
		M.RequireACKs[sid]++
	}
	M.Unlock()
}
func (M *ACKMap) popACK(sid int)  {
	M.Lock()
	if M.RequireACKs[sid] > 0 {
		M.RequireACKs[sid]--
	}
	M.Unlock()
}

func (M *ACKMap) isEmpty() bool {
	M.RLock()
	for _, num := range M.RequireACKs {
		if num > 0 {
			//log.PrintfACKMap非空("ACKMap非空：%v\n", M.RequireACKs)
			//log.Printf("%d：%v\n", i, num)
			M.RUnlock()
			return false
		}
	}
	M.RUnlock()
	return true
}


type ACKIPMap struct {
	sync.RWMutex
	ACKReceiverIPs map[int]string
}
func (M *ACKIPMap) getIP(sid int) (string, bool)  {
	M.RLock()
	ip, ok := M.ACKReceiverIPs[sid]
	M.RUnlock()
	return ip, ok
}
func (M *ACKIPMap) recordIP(sid int, ip string)  {
	M.Lock()
	M.ACKReceiverIPs[sid] = ip
	M.Unlock()
}

var CurPolicy Policy = nil
//var CurPolicyType int = 0
var ackMaps *ACKMap
var ackIPMaps *ACKIPMap
var sid = 0
var totalCrossRackTraffic = 0
var ReceivedAckCh = make(chan config.ACK, 10)
var ReceivedTDCh = make(chan config.TD, 10)
var ReceivedCMDCh = make(chan config.CMD, 10)
func SetPolicy(policyType config.PolicyType)  {
	switch policyType {
	case config.BASE:
		CurPolicy = Base{}
	case config.BASEMulticast:
		CurPolicy = BaseMulticast{}
	case config.T_Update:
		CurPolicy = TUpdate{}
	case config.T_Update1:
		CurPolicy = TUpdate1{}
	case config.CAU:
		CurPolicy = CAU{}
	case config.CAU1:
		CurPolicy = CAU1{}
	case config.CAURS:
		CurPolicy = CAURS{}
	//case config.Forest:
	//	CurPolicy = Forest{}
	case config.TAR_CAU:
		CurPolicy = TAR_CAU{}
	}
	CurPolicy.Init()
}
func GetCurPolicy() Policy {
	//if CurPolicy == nil {
	//	//fmt.Println("CurPolicy is nil！")
	//}
	return CurPolicy
}
func (p Base) HandleCMD(cmd *config.CMD) {
	handleOneCMD(cmd)
}

func handleOneCMD(cmd *config.CMD)  {
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID, config.BlockSize)
	//buff := common.ReadBlock(cmd.BlockID)
	log.Printf("读取到数据 block %d: %v\n", cmd.BlockID, len(buff))
	for _, _ = range cmd.ToIPs {
		ackMaps.pushACK(cmd.SID)
	}
	//var td config.TD
	for _, parityIP := range cmd.ToIPs {
		td := &config.TD{
			BlockID: cmd.BlockID,
			Buff: buff[:config.BlockSize],
			FromIP: cmd.FromIP,
			ToIP: parityIP,
			SID: cmd.SID,
			SendSize: cmd.SendSize,
		}
		//td := config.TDBufferPool.Get().(*config.TD)
		//td.BlockID = cmd.BlockID
		//td.Buff = buff[:config.BlockSize]
		//td.FromIP = cmd.FromIP
		//td.ToIP = parityIP
		//td.SID = cmd.SID
		//td.SendSize = config.BlockSize
		//common.SendData(td, parityIP, config.NodeTDListenPort, "")

		begin := time.Now()
		common.SendData(td, parityIP, config.NodeTDListenPort, "")
		elapsed := time.Since(begin)

		log.Printf("发送 td(sid: %d, blockID: %d), 从 %s 到 %s, 数据量：%d MB，  用时：%.2v \n",
			cmd.SID, cmd.BlockID, common.GetLocalIP(), parityIP, 1.0 * td.SendSize/config.Megabyte, elapsed)

		//config.TDBufferPool.Put(td)
	}
	config.BlockBufferPool.Put(buff)
}
func (p Base) HandleTD(td *config.TD)  {
	handleOneTD(td)
}
func handleOneTD(td *config.TD)  {
	go common.WriteDeltaBlock(td.BlockID, td.Buff)
	//返回ack
	ack := config.AckBufferPool.Get().(*config.ACK)
	ack.SID = td.SID
	ack.BlockID = td.BlockID

	ReturnACK(ack)

	config.AckBufferPool.Put(ack)

}
func (p Base) HandleACK(ack *config.ACK)  {
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
func ReturnACK(ack *config.ACK) {
	if ackReceiverIP, ok := ackIPMaps.getIP(ack.SID); ok{
		common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")
		log.Printf("任务已完成，给上级：%s返回ack: sid: %d, blockID: %d\n", ackReceiverIP, ack.SID, ack.BlockID)
	}
	//else{
	//	log.Fatal("returnACK error! ack: ", ack, " ackReceiverIPs: ", ackIPMaps)
	//}

	//config.AckBufferPool.Put(ack)
}

func (p Base) Init()  {
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

func getBatchReqs() []*config.ReqData {
	//获取curDistinctBlocks
	curMatchReqs := make([]*config.ReqData, 0, config.MaxBaseBatchSize)
	if len(totalReqs) > config.MaxBaseBatchSize {
		curMatchReqs = totalReqs[:config.MaxBaseBatchSize]
		totalReqs = totalReqs[config.MaxBaseBatchSize:]
	}else { //处理最后不到100个请求
		curMatchReqs = totalReqs
		totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
	}
	return curMatchReqs
}

func (p Base) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	_ = reqs

	for len(totalReqs) > 0 {
		//过滤blocks
		batchReqs := getBatchReqs()
		actualBlocks += len(batchReqs)
		log.Printf("第%d轮 BASE：处理%d个block\n", round, len(batchReqs))
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
func (p Base) base(reqs []*config.ReqData)  {
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
func (p Base) handleOneBlock(reqData config.ReqData)  {
	nodeID := common.GetNodeID(reqData.BlockID)
	fromIP := common.GetNodeIP(nodeID)
	toIPs := common.GetRelatedParityIPs(reqData.BlockID)
	common.SendCMD(fromIP, toIPs, reqData.SID, reqData.BlockID)
	//跨域流量统计
	totalCrossRackTraffic += len(toIPs) * config.BlockSize
	log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", reqData.SID,
		nodeID, common.GetNodeIP(nodeID), reqData.BlockID, toIPs)

}
func (p Base) RecordSIDAndReceiverIP(sid int, ip string)  {
	ackIPMaps.recordIP(sid, ip)
}
func (p Base) Clear()  {
	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	IsRunning = true
}
func ACKIsEmpty() bool {
	return ackMaps.isEmpty()
}

func (p Base) IsFinished() bool {
	isFinished := len(totalReqs) == 0 && ackMaps.isEmpty()
	return isFinished
}

func (p Base) GetActualBlocks() int {
	return actualBlocks
}
//数据格式：MB
func GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.Megabyte
}

