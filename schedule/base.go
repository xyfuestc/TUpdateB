package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
	"time"
)

type Policy interface {
	Init()
	HandleReq(reqs []config.ReqData)
	HandleCMD(cmd config.CMD)
	HandleTD(td config.TD)
	HandleACK(ack config.ACK)
	Clear()
	RecordSIDAndReceiverIP(sid int, ip string)
}

type Base struct {

}
var CurPolicy Policy = nil
var AckReceiverIPs = make(map[int]string)
var RequireACKs = make([]int, 0, 1000000)
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
func (p Base) HandleCMD(cmd config.CMD) {
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID)

	for _, _ = range cmd.ToIPs {
		pushACK(cmd.SID)
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
	if arrays.Contains(RequireACKs, sid) < 0 {
		RequireACKs = append(RequireACKs, sid)
	}
	RequireACKs[sid]++
}

func popACK(sid int)  {
	RequireACKs[sid]--
}

func (p Base) HandleTD(td config.TD)  {
	go common.WriteBlock(td.BlockID, td.Buff)
	//返回ack
	ack := config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	ReturnACK(ack)
}
func (p Base) HandleACK(ack config.ACK)  {
	fmt.Printf("当前剩余ack：%d\n", RequireACKs)
	popACK(ack.SID)
	if RequireACKs[ack.SID] == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}
	}
}
func ReturnACK(ack config.ACK) {
	ackReceiverIP := AckReceiverIPs[ack.SID]
	common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")
	fmt.Printf("任务已完成，给上级：%s返回ack: sid: %d, blockID: %d\n", ackReceiverIP, ack.SID, ack.BlockID)
}

func (p Base) Init()  {
}

func (p Base) HandleReq(reqs []config.ReqData)  {

	for _, req := range reqs{
		RequireACKs[req.SID]++
	}

	for _, req := range reqs{
		p.handleOneBlock(req)
	}
}
func (p Base) handleOneBlock(reqData config.ReqData)  {
	nodeID := common.GetNodeID(reqData.BlockID)
	relativeParityIDs := common.RelatedParities(reqData.BlockID)
	cmd := common.GetCMDFromReqData(reqData)

	fmt.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v (%v)\n", reqData.SID,
		nodeID, common.GetNodeIP(nodeID), reqData.BlockID, relativeParityIDs, cmd.ToIPs)
	common.SendData(cmd, common.GetNodeIP(nodeID), config.NodeCMDListenPort, "")
}
func (p Base) RecordSIDAndReceiverIP(sid int, ip string)  {
	AckReceiverIPs[sid] = ip
}
func (p Base) Clear()  {
	AckReceiverIPs = make(map[int]string)
	RequireACKs = make([]int, 0, 1000000)
}
func ACKIsEmpty() bool {
	for _, num := range RequireACKs {
		if num > 0 {
			return false
		}
	}
	return true
}


