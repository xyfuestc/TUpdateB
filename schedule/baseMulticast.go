package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"log"
	"sync"
	"time"
)

/*BaseMulticast: delta + handle one block + XOR + star-structured + multicast */
type BaseMulticast struct {

}
type MsgLogMap struct {
	sync.RWMutex
	MsgLog map[int]config.MTU
	countLog map[int]int
}
func (M *MsgLogMap) getMsg(sid int) (config.MTU, bool)  {
	M.RLock()
	msg, ok := M.MsgLog[sid]
	M.RUnlock()
	return msg, ok
}
func (M *MsgLogMap) Init()  {
	M.Lock()
	M.MsgLog = make(map[int]config.MTU)
	M.Unlock()
}
func (M *MsgLogMap) getAllMsg() map[int]config.MTU  {
	msg := map[int]config.MTU{}
	M.RLock()
	msg = M.MsgLog
	M.RUnlock()
	return msg
}
func (M *MsgLogMap) PushMsg(sid int, msg config.MTU)  {
	M.Lock()
	M.MsgLog[sid] = config.MTU{}
	M.MsgLog[sid] = msg
	M.countLog[sid] = len(msg.MultiTargetIPs)
	M.Unlock()
}
func (M *MsgLogMap) popMsg(sid int)  {
	M.Lock()
	if _, ok := M.MsgLog[sid]; ok {
		M.countLog[sid]--
		if M.countLog[sid] == 0 {
			delete(M.MsgLog, sid)
			delete(M.countLog, sid)
		}
	}
	M.Unlock()
}

func (M *MsgLogMap) isEmpty() bool {
	M.RLock()
	l := len(M.MsgLog) == 0
	M.RUnlock()
	return l
}
var MulticastSendMTUCh = make(chan config.MTU)
var MulticastReceiveMTUCh = make(chan config.MTU, 100)
var MulticastReceiveAckCh = make(chan config.ACK)
var SentMsgLog MsgLogMap
func (p BaseMulticast) HandleCMD(cmd *config.CMD) {
	//重复SID，不处理
	if _, ok := ackMaps.getACK(cmd.SID); ok {
		return
	}
	//利用多播将数据发出
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID, cmd.SendSize)

	//buff := common.ReadBlock(cmd.BlockID)
	//log.Printf("读取到数据 block %d: size: %v\n", cmd.BlockID, len(buff))
	for _, _ = range cmd.ToIPs {
		ackMaps.pushACK(cmd.SID)
	}
	//fragments := GetFragments(cmd)
	//for _, f := range fragments {
	//	MulticastSendMTUCh <- *f
	//}
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
	//time.Sleep(config.UDPDuration)
	SentMsgLog.PushMsg(message.SID, *message) //记录block

	config.BlockBufferPool.Put(buff)
	//SendMessageAndWaitingForACK(message)
	log.Printf("HandleCMD: 发送td(sid:%d, blockID:%d)，从%s到%v \n", cmd.SID, cmd.BlockID, common.GetLocalIP(), cmd.ToIPs)

}
//处理UDP超时,返回处理msg数量
func HandleTimeout() int {
	count := 0
	msgLog := SentMsgLog.getAllMsg()
	for sid, msg := range msgLog{
		count++
		MulticastSendMTUCh <- msg
		log.Printf("重发: sid:%v, blockID:%v", sid, msg.BlockID)
	}
	return count
}
func (p BaseMulticast) HandleTD(td *config.TD)  {
	handleOneTD(td)
}
func (p BaseMulticast) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		SentMsgLog.popMsg(ack.SID)      //该SID重发数-1
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
	sid = 0
	SentMsgLog.Init()
}


func (p BaseMulticast) HandleReq(reqs []*config.ReqData)  {
	totalReqs = reqs

	for len(totalReqs) > 0 {

		batchReqs := getBatchReqs()
		actualBlocks += len(batchReqs)
		log.Printf("第%d轮 BaseMulticast：处理%d个block\n", round, len(batchReqs))
		//执行base
		p.baseMulti(batchReqs)

		for IsRunning {

		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}


}
func (p BaseMulticast) baseMulti(reqs []*config.ReqData)  {
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
	}
	common.SendData(cmd, fromIP, config.NodeCMDListenPort)

	//跨域流量统计
	totalCrossRackTraffic += len(toIPs) * (rangeRight - rangeLeft)
	log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v. SendSize: %v\n", reqData.SID,
		nodeID, common.GetNodeIP(nodeID), reqData.BlockID, toIPs, cmd.SendSize)
}
func (p BaseMulticast) RecordSIDAndReceiverIP(sid int, ip string)  {
	ackIPMaps.recordIP(sid, ip)
}
func ClearChannels()  {
	//清空ACK
	select {
	case <-MulticastReceiveAckCh:
	case <-MulticastReceiveMTUCh:
	case <-MulticastSendMTUCh:
	default:
	}
}

func (p BaseMulticast) Clear()  {

	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}

	ClearChannels()


	IsRunning = true
	//清空SentMsgLog
	SentMsgLog.Init()

}
func (p BaseMulticast) IsFinished() bool {
	isFinished :=  len(totalReqs) == 0 && ackMaps.isEmpty()
	if isFinished {
		//CloseAllChannels()
	}
	return isFinished
}
func IsClosed(ch <-chan interface{}) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

func CloseAllChannels()  {
	//base
	_, beforeClosed := <-ReceivedAckCh
	if !beforeClosed {
		fmt.Println("ReceivedAckCh has been closed")
	}else{
		close(ReceivedAckCh)
	}

	_, beforeClosed = <-ReceivedTDCh
	if !beforeClosed {
		fmt.Println("ReceivedTDCh has been closed")
	}else{
		close(ReceivedTDCh)
	}

	_, beforeClosed = <-ReceivedCMDCh
	if !beforeClosed {
		fmt.Println("ReceivedCMDCh has been closed")
	}else {
		close(ReceivedCMDCh)
	}

	_, beforeClosed = <-MulticastReceiveAckCh
	if !beforeClosed {
		fmt.Println("MulticastReceiveAckCh has been closed")
	}else {
		close(MulticastReceiveAckCh)
	}

	_, beforeClosed = <-MulticastSendMTUCh
	if !beforeClosed {
		fmt.Println("MulticastSendMTUCh has been closed")
	}else{
		close(MulticastSendMTUCh)
	}

	_, beforeClosed = <-MulticastReceiveMTUCh
	if !beforeClosed {
		fmt.Println("MulticastReceiveMTUCh has been closed")
	}else{
		close(MulticastReceiveMTUCh)
	}


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
func GetFragments(cmd *config.CMD) []*config.MTU {
	//1.获取数据
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID, cmd.SendSize)

	//2.发送数据
	count := cmd.SendSize / config.MTUSize
	fragments := make([]*config.MTU, 0, count)
	var sendData []byte

	if count > 1 { //分片发送数据
		for index := 0; index < count+1; index++ {
			length := 0
			if index == count { // 处理最后一个分片
				length = cmd.SendSize - index*config.MTUSize
			} else {
				length = config.MTUSize
			}
			//如果刚好除尽，最后不用处理
			if length == 0 {
				break
			}
			sendData = buff[index*config.MTUSize : index*config.MTUSize+length]
			message := &config.MTU{
				BlockID:        cmd.BlockID,
				Data:           sendData,
				FromIP:         cmd.FromIP,
				MultiTargetIPs: cmd.ToIPs,
				SID:            cmd.SID,
				IsFragment:     true,
				FragmentID:     index,
				FragmentCount:  count,
			}
			fragments = append(fragments, message)
			//MulticastSendMTUCh <- *message
			//select {
			//case ack := <-MulticastReceiveAckCh:
			//	fmt.Printf("SID %v: Frag %v send success.\n", message.SID, message.FragmentID)
			//case <-time.After(time.Second):
			//	fmt.Printf("timeout! SID %v: Frag %v send failed.\n", message.SID, message.FragmentID)
			//	index--
			//	continue
			//}
			//time.Sleep(500 * time.Millisecond)
		}

		//记录
		//var builder strings.Builder
		//builder.WriteString(strconv.Itoa(message.SID))
		//builder.WriteString(strconv.Itoa(message.FragmentID))
		//SentMsgLog[builder.String()] = *message

		//log.Printf("发送sid: %v的第%v（共%d）个分片数据.", cmd.SID, index, count)
	} else { //数据量小，不需要分片
		//for {
			message := &config.MTU{
				BlockID:        cmd.BlockID,
				Data:           buff[:cmd.SendSize],
				FromIP:         cmd.FromIP,
				MultiTargetIPs: cmd.ToIPs,
				SID:            cmd.SID,
				FragmentID:     0,
				IsFragment:     false,
				SendSize: cmd.SendSize,
			}
			fragments = append(fragments, message)
			//MulticastSendMTUCh <- *message
			//select {
			//case <-MulticastReceiveAckCh:
			//	fmt.Printf("SID %v: Frag %v send success.\n", message.SID, message.FragmentID)
			//	break
			//case <-time.After(time.Second):
			//	fmt.Printf("timeout! SID %v: Frag %v send failed.\n", message.SID, message.FragmentID)
			//	continue
			//}
			//time.Sleep(500 * time.Millisecond)
		//}
	}
	config.BlockBufferPool.Put(buff)
	return fragments
}
func SendMessageAndWaitingForACK(message *config.MTU)  {
	MulticastSendMTUCh <- *message
	//for {
		//确认收到ack
		select {
		case ack := <-MulticastReceiveAckCh:
			fmt.Printf("确认收到ack: %+v\n", ack)
			break
		case <-time.After(2 * time.Millisecond):
			fmt.Printf("%v ack返回超时！\n", message.SID)
			//SendMessageAndWaitingForACK(message)
			MulticastSendMTUCh <- *message
		}
	//}
}