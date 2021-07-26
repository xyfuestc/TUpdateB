package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"log"
)

type Policy interface {
	Init()
	HandleReq(reqData config.ReqData)
	HandleCMD(cmd config.CMD)
	HandleTD(td config.TD)
	HandleACK(ack config.ACK)
	Clear()
}

type Base struct {

}

var WaitingACKGroup = make(map[int]*config.WaitingACKItem)
var CurPolicy Policy = nil
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
	PushWaitingACKGroup(cmd.SID, cmd.BlockID, len(cmd.ToIPs), cmd.CreatorIP, "")
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
	}

}

func PushWaitingACKGroup(sid, blockID, requiredACKNum int, ackReceiverIP, ackSenderIP string)  {
	if _, ok := WaitingACKGroup[sid]; !ok {
		WaitingACKGroup[sid] = &config.WaitingACKItem{BlockID: blockID, SID: sid,
			ACKReceiverIP: ackReceiverIP, ACKSenderIP: ackSenderIP, RequiredACK: requiredACKNum}
	}else{
		WaitingACKGroup[sid].RequiredACK = WaitingACKGroup[sid].RequiredACK + requiredACKNum
	}
	PrintWaitingACKGroup("After PushWaitingACKGroup : ")
}

func PopWaitingACKGroup(sid int)  {
	if _, ok := WaitingACKGroup[sid]; !ok {
		log.Fatalln("popWaitingACKGroup error : sid is invalid. ")
	}else{
		WaitingACKGroup[sid].RequiredACK = WaitingACKGroup[sid].RequiredACK - 1
	}
	PrintWaitingACKGroup("After PopWaitingACKGroup : ")
}

func PrintWaitingACKGroup(prefix string)  {
	for i, v := range WaitingACKGroup{
		fmt.Printf("%s sid : %d, blockID :%d, still need %d ack.\n", prefix, i, v.BlockID, v.RequiredACK)
	}
}
func IsExistInWaitingACKGroup(sid int) bool  {
	if WaitingACKGroup[sid].RequiredACK > 0 {
		return true
	}
	return false
}
func ClearWaitingACKGroup()  {
	WaitingACKGroup = make(map[int]*config.WaitingACKItem)
}
func (p Base) HandleTD(td config.TD)  {
	common.WriteBlock(td.BlockID, td.Buff)

	ack := &config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	common.SendData(ack, td.FromIP, config.NodeACKListenPort, "ack")
}
func (p Base) HandleACK(ack config.ACK)  {
	PopWaitingACKGroup(ack.SID)
	if NeedReturnACK(ack) {
		ReturnACK(ack)
	}
}
func ReturnACK(ackV config.ACK) {
	ack := &config.ACK{
		SID:     ackV.SID,
		BlockID: ackV.BlockID,
	}
	ackReceiverIP := WaitingACKGroup[ack.SID].ACKReceiverIP
	common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")

	delete(WaitingACKGroup, ack.SID)
}
func NeedReturnACK(ack config.ACK) bool {
	if !IsExistInWaitingACKGroup(ack.SID) &&
		WaitingACKGroup[ack.SID].ACKReceiverIP != common.GetLocalIP() {
			return true
	}
	return false
}
func (p Base) Init()  {
}

func (p Base) HandleReq(reqData config.ReqData)  {
	sid := reqData.SID
	blockID := reqData.BlockID
	stripeID := reqData.StripeID
	nodeID := common.GetNodeID(blockID)
	nodeIP := common.GetNodeIP(nodeID)
	relativeParityIDs := common.GetRelatedParities(blockID)
	toIPs := common.GetRelatedParityIPs(blockID)
	cmd := config.CMD{
		CreatorIP: common.GetLocalIP(),
		SID:       sid,
		Type:      config.CMD_BASE,
		StripeID:  stripeID,
		BlockID:   blockID,
		ToIPs:     toIPs,
		FromIP:    nodeIP,
	}
	fmt.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid, nodeID, nodeIP, blockID, relativeParityIDs)
	common.SendData(cmd, nodeIP, config.NodeCMDListenPort, "")
	PushWaitingACKGroup(cmd.SID, cmd.BlockID,1, cmd.CreatorIP, nodeIP)
}

func IsEmptyInWaitingACKGroup() bool  {
	if len(WaitingACKGroup) == 0 {
		return true
	}
	return false
}

func (p Base) Clear()  {

}


