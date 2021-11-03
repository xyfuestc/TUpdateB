package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"log"
	"sync"
	"time"
)

type Policy interface {
	Init()
	HandleReq(blocks []int)
	HandleCMD(cmd *config.CMD)
	HandleTD(td *config.TD)
	HandleACK(ack *config.ACK)
	Clear()
	RecordSIDAndReceiverIP(sid int, ip string)
	IsFinished() bool
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
	M.RequireACKs[sid]--
	M.Unlock()
}

func (M *ACKMap) isEmpty() bool {
	M.RLock()
	for _, num := range M.RequireACKs {
		if num > 0 {
			//fmt.Printf("ACKMap非空：%v\n", M.RequireACKs)
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
var ackMaps *ACKMap
var ackIPMaps *ACKIPMap
var sid = 0
func SetPolicy(policyType config.PolicyType)  {
	switch policyType {
	case config.BASE:
		CurPolicy = Base{}
	case config.CAU:
		CurPolicy = CAU{}
	case config.T_Update:
		CurPolicy = TUpdate{}
	case config.DPR_Forest:
		CurPolicy = Forest{}
	}
	CurPolicy.Init()
}
func GetCurPolicy() Policy {
	if CurPolicy == nil  {
		SetPolicy(config.CurPolicyVal)
	}
	return CurPolicy
}
func (p Base) HandleCMD(cmd *config.CMD) {
	handlOneCMD(cmd)
}

func handlOneCMD(cmd *config.CMD)  {
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID)
	for _, _ = range cmd.ToIPs {
		ackMaps.pushACK(cmd.SID)
	}
	for _, parityIP := range cmd.ToIPs{
		td := &config.TD{
			BlockID: cmd.BlockID,
			Buff: buff,
			FromIP: cmd.FromIP,
			ToIP: parityIP,
			SID: cmd.SID,
		}
		begin := time.Now().UnixNano() / 1e6
		common.SendData(td, parityIP, config.NodeTDListenPort, "")
		end := time.Now().UnixNano() / 1e6
		fmt.Printf("发送td(sid:%d, blockID:%d),从%s到%s, 用时：%vms \n", cmd.SID, cmd.BlockID, common.GetLocalIP(), parityIP, end-begin)
	}
}

func pushACK(sid int)  {


}

//func popACK(pushACKsid int)  {
//	RequireACKs[sid]--
//}

func (p Base) HandleTD(td *config.TD)  {
	handleOneTD(td)
}
func handleOneTD(td *config.TD)  {
	go common.WriteDeltaBlock(td.BlockID, td.Buff)
	//返回ack
	ack := &config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	ReturnACK(ack)
}
func (p Base) HandleACK(ack *config.ACK)  {
	//fmt.Printf("当前剩余ack：%d\n", RequireACKs)
	ackMaps.popACK(ack.SID)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}
	}
}
func ReturnACK(ack *config.ACK) {
	if ackReceiverIP, ok := ackIPMaps.getIP(ack.SID); ok{
		common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")
		fmt.Printf("任务已完成，给上级：%s返回ack: sid: %d, blockID: %d\n", ackReceiverIP, ack.SID, ack.BlockID)
	}else{
		log.Fatal("returnACK error! ack: ", ack, " ackReceiverIPs: ", ackIPMaps)
	}
}

func (p Base) Init()  {
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
}

func (p Base) HandleReq(blocks []int)  {

	for _, _ = range blocks {
		ackMaps.pushACK(sid)
		sid++
	}

	sid = 0
	for _, b := range blocks {
		req := config.ReqData{
			BlockID: b,
			SID: sid,
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

	fmt.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", reqData.SID,
		nodeID, common.GetNodeIP(nodeID), reqData.BlockID, toIPs)
}
func (p Base) RecordSIDAndReceiverIP(sid int, ip string)  {
	ackIPMaps.recordIP(sid, ip)
}
func (p Base) Clear()  {
	sid = 0
	//RequireACKs = make(map[int]int)
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
}
func ACKIsEmpty() bool {
	return ackMaps.isEmpty()
}

func (p Base) IsFinished() bool {
	return ackMaps.isEmpty()
}

