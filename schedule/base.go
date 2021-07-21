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
		common.SendData(td, parityIP, config.NodeTDListenPort, "")
		PushWaitingACKGroup(cmd.SID, cmd.BlockID, cmd.FromIP, parityIP)
	}
}

func PushWaitingACKGroup(sid, blockID int, ackReceiverIP, ackSenderIP string)  {
	if _, ok := WaitingACKGroup[sid]; !ok {
		WaitingACKGroup[sid] = &config.WaitingACKItem{BlockID: blockID, SID: sid,
			ACKReceiverIP: ackReceiverIP, ACKSenderIP: ackSenderIP, RequiredACK: 1}
	}else{
		WaitingACKGroup[sid].RequiredACK = WaitingACKGroup[sid].RequiredACK + 1
	}
}

func PopWaitingACKGroup(sid int)  {
	if _, ok := WaitingACKGroup[sid]; !ok {
		log.Fatalln("popWaitingACKGroup error : sid is invalid. ")
	}else{
		WaitingACKGroup[sid].RequiredACK = WaitingACKGroup[sid].RequiredACK - 1
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
	if !IsExistInWaitingACKGroup(ack.SID) {
		ack := &config.ACK{
			SID:     ack.SID,
			BlockID: ack.BlockID,
		}
		ackReceiverIP := WaitingACKGroup[ack.SID].ACKReceiverIP
		common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")

		delete(WaitingACKGroup, ack.SID)
	}
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
		SID:      sid,
		Type:     config.CMD_BASE,
		StripeID: stripeID,
		BlockID:  blockID,
		ToIPs:    toIPs,
		FromIP:   nodeIP,
	}
	fmt.Printf("发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", nodeID, nodeIP, blockID, relativeParityIDs)
	common.SendData(cmd, nodeIP, config.NodeCMDListenPort, "")
	PushWaitingACKGroup(cmd.SID, cmd.BlockID, cmd.FromIP, "")
}

func IsEmptyInWaitingACKGroup() bool  {
	if len(WaitingACKGroup) == 0 {
		return true
	}
	return false
}

func (p Base) Clear()  {

}


