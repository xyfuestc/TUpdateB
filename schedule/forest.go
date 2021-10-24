package schedule

import (
	"EC/config"
)

type Forest struct {
	Base
}

func (p Forest) Init()  {

}
func (p Forest) HandleReq(reqs []config.ReqData)  {

}
func (p Forest) HandleTD(td config.TD)  {

}
func (p Forest) HandleCMD(cmd config.CMD)  {

}
func (p Forest) HandleACK(ack config.ACK)  {
	//popACK(ack.SID)
	//if !IsExistInWaitingACKGroup(ack.SID) {
	//	ack := &config.ACK{
	//		SID:     ack.SID,
	//		BlockID: ack.BlockID,
	//	}
	//	ackReceiverIP := WaitingACKGroup[ack.SID].ACKReceiverIP
	//	common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")
	//
	//	delete(WaitingACKGroup, ack.SID)
	//}
}
func (p Forest) Clear()  {

}

func (p Forest)	RecordSIDAndReceiverIP(sid int, ip string)()  {

}