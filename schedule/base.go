package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
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
var RequireACKs = 0
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

	for _, parityIP := range cmd.ToIPs{
		td := &config.TD{
			BlockID: cmd.BlockID,
			Buff: buff,
			FromIP: cmd.FromIP,
			ToIP: parityIP,
			SID: cmd.SID,
		}
		fmt.Printf("send td(sid:%d, blockID:%d) to %s\n", cmd.SID, cmd.BlockID, parityIP)
		common.SendData(td, parityIP, config.NodeTDListenPort, "")

		pushACK()
	}
}
func pushACK()  {
	RequireACKs++
}

func popACK()  {
	RequireACKs--
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
	fmt.Printf("收到ack: sid: %d, id: %d\n", ack.SID, ack.BlockID)
	popACK()
	if NeedReturnACK() {
		ReturnACK(ack)
	}
}
func ReturnACK(ack config.ACK) {
	ackReceiverIP := AckReceiverIPs[ack.SID]
	common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")
	fmt.Printf("任务已完成，给上级：%s返回ack: sid: %d, id: %d\n", ackReceiverIP, ack.SID, ack.BlockID)

}
func NeedReturnACK() bool {
	if RequireACKs == 0  {
			return true
	}
	return false
}
func (p Base) Init()  {
}

func (p Base) HandleReq(reqs []config.ReqData)  {

	RequireACKs = len(reqs)
	for _, req := range reqs{
		p.handleOneBlock(req)
	}
}
func (p Base) handleOneBlock(reqData config.ReqData)  {
	nodeID := common.GetNodeID(reqData.BlockID)
	relativeParityIDs := common.RelatedParities(reqData.BlockID)
	cmd := common.GetCMDFromReqData(reqData)

	fmt.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", reqData.SID,
		nodeID, common.GetNodeIP(nodeID), reqData.BlockID, relativeParityIDs)
	common.SendData(cmd, common.GetNodeIP(nodeID), config.NodeCMDListenPort, "")
}
func (p Base) RecordSIDAndReceiverIP(sid int, ip string)  {
	AckReceiverIPs[sid] = ip
}
func (p Base) Clear()  {
	AckReceiverIPs = make(map[int]string)
}

