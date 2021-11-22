package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"
	"sync"
)
type TAR_CAU struct {
	Base
}

type ReceivedTDs struct {
	sync.RWMutex
	TDs []*config.TD
}
func (M *ReceivedTDs) pushTD(td *config.TD)  {
	M.Lock()
	M.TDs = append(M.TDs, td)
	M.Unlock()
}
func (M *ReceivedTDs) isEmpty() bool  {
	M.RLock()
	num := len(M.TDs)
	M.RUnlock()
	if num > 0 {
		return false
	}
	return true
}
func (M *ReceivedTDs) getTDs() []*config.TD  {
	M.RLock()
	num := len(M.TDs)
	M.RUnlock()
	if num > 0 {
		return M.TDs
	}
	return nil
}
var curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
var curReceivedTDs *ReceivedTDs
func (p TAR_CAU) Init()  {
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	actualBlocks = 0
	round = 0
	curReceivedTDs = &ReceivedTDs{
		TDs: make([]*config.TD, 0, config.MaxBatchSize),
	}
}
func (p TAR_CAU) HandleTD(td *config.TD) {
	//记录当前轮次接收到的blockID
	curReceivedTDs.pushTD(td)
	//校验节点本地数据更新
	localID := arrays.Contains(config.NodeIPs, common.GetLocalIP())
	if localID >= config.K {
		go common.WriteDeltaBlock(td.BlockID, td.Buff)
	}
	//返回ack
	ack := &config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	ReturnACK(ack)

	handleWaitingCMDs(td)
}

func handleWaitingCMDs(td *config.TD) {
	//有等待任务
	indexes := meetCMDNeed(td.BlockID)
	if len(indexes) > 0 {
		for _, i := range indexes {
			cmd := i
			fmt.Printf("执行TD任务：sid:%d blockID:%d\n", cmd.SID, cmd.BlockID)
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		for _, i := range indexes {
			cmd := i
			for _, toIP := range cmd.ToIPs {
				td := &config.TD{
					BlockID: cmd.BlockID,
					Buff:    td.Buff,
					FromIP:  cmd.FromIP,
					ToIP:    toIP,
					SID:     cmd.SID,
					SendSize: cmd.SendSize,
				}
				sendSizeRate := float32(td.SendSize * 1.0) / float32(config.BlockSize)
				fmt.Printf("发送 block:%d sendSize: %.2f 的数据给%s.\n", td.BlockID, sendSizeRate, toIP)
				common.SendData(td, toIP, config.NodeTDListenPort, "")
			}
		}
	}
}

func (p TAR_CAU) HandleReq(reqs []*config.ReqData)  {
	totalReqs = reqs
	fmt.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		findDistinctReqs()
		//执行cau
		actualBlocks += len(curDistinctReq)
		fmt.Printf("第%d轮 TAR-CAU：处理%d个block\n", round, len(curDistinctReq))

		tar_cau()

		for IsRunning {
			
		}
		fmt.Printf("本轮结束！\n")
		fmt.Printf("======================================\n")
		round++
		p.Clear()
	}
	//p.Clear()

}

func turnMatchReqsToDistinctReqs(curMatchReqs []*config.ReqData)  {
	for _, req := range curMatchReqs {
		if i := findBlockIndexInReqs(curDistinctReq, req.BlockID); i < 0 {
			curDistinctReq = append(curDistinctReq, req)
		}else{
			if req.RangeLeft < curDistinctReq[i].RangeLeft {
				curDistinctReq[i].RangeLeft = req.RangeLeft
			}else if req.RangeRight > curDistinctReq[i].RangeRight {
				curDistinctReq[i].RangeRight = req.RangeRight
			}
		}
	}
}
func findDistinctReqs() {
	//获取curDistinctBlocks
	curMatchReqs := make([]*config.ReqData, 0, config.MaxBatchSize)
	if len(totalReqs) > config.MaxBatchSize {
		curMatchReqs = totalReqs[:config.MaxBatchSize]
		turnMatchReqsToDistinctReqs(curMatchReqs)
		totalReqs = totalReqs[config.MaxBatchSize:]
	}else { //处理最后不到100个请求
		curMatchReqs = totalReqs
		turnMatchReqsToDistinctReqs(curMatchReqs)
		totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
	}
}

func findBlockIndexInReqs(reqs []*config.ReqData, blockID int) int {
	for i, req := range reqs {
		if req.BlockID == blockID {
			return i
		}
	}
	return -1
}
func turnReqsToStripes() map[int][]int {
	stripes := map[int][]int{}
	for _, req := range curDistinctReq {
		stripeID := common.GetStripeIDFromBlockID(req.BlockID)
		stripes[stripeID] = append(stripes[stripeID], req.BlockID)
	}
	return  stripes
}

func tar_cau() {
	stripes := turnReqsToStripes()
	for _, stripe := range stripes{
		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {
				if compareRacks(i, ParityRackIndex, stripe) {
					parityUpdate1(i, stripe)
				}else{
					dataUpdate1(i, stripe)
				}
			}
		}
	}
}
func dataUpdate1(rackID int, stripe []int)  {
	curRackNodes := make([][]int, config.RackSize)
	parities := make([][]int, config.M * config.W)
	for _, blockID := range stripe{
		nodeID := common.GetNodeID(blockID)
		if byte(rackID) != getRackIDFromNodeID(byte(nodeID)) {
			continue
		}
		//fmt.Printf("blockID: %d, nodeID: %d, rackID: %d\n", blockID, nodeID, rackID)
		curRackNodes[nodeID-rackID*config.RackSize] = append(curRackNodes[nodeID-rackID*config.RackSize], blockID)
		for _, p := range common.RelatedParities(blockID){
			if arrays.Contains(parities[p], blockID) < 0 {
				parities[p] = append(parities[p], blockID)
			}
		}
	}
	unionParities := make([]int, 0, config.K * config.W)
	for _, p := range parities {
		unionParities = common.Union(p, unionParities)
	}
	if len(unionParities) == 0 {
		return
	}

	//选择一个rootP
	rootP := GetRootParityID(parities)
	if rootP < 0 {
		log.Fatal("找不到rootParity")
		return
	}

	curSid := sid
	/****记录ack*****/
	parityNodeBlocks := GetParityNodeBlocks(parities)
	for i, b := range parityNodeBlocks {
		parityID := i + config.K
		if parityID != rootP && len(b) > 0 {
			fmt.Printf("pushACK: sid: %d, blockID: %v\n", curSid, b)
			ackMaps.pushACK(curSid)
			curSid++
		}
	}
	/****分发*****/
	fmt.Printf("DataUpdate: parityNodeBlocks: %v\n", parityNodeBlocks)
	for i, blocks := range parityNodeBlocks {
		parityID := i + config.K
		if parityID != rootP {
			//传输blocks到rootD
			for _, b := range blocks{
				//省略了合并操作，直接只发一条
				fmt.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
					rootP, common.GetNodeIP(rootP), b, common.GetNodeIP(parityID))
				//rangeLeft,rangeRight := getRangeFromBlockID(b)
				cmd := &config.CMD{
					SID: sid,
					BlockID: b,
					ToIPs: []string{common.GetNodeIP(parityID)},
					FromIP: common.GetNodeIP(rootP),
					Helpers: blocks,
					Matched: 0,
					//SendSize: rangeRight-rangeLeft,
					SendSize: config.BlockSize,
				}
				common.SendData(cmd, common.GetNodeIP(rootP), config.NodeCMDListenPort, "")

				sid++
				break
			}
		}
	}

	for _, blocks := range curRackNodes {
		//传输blocks到rootP
		for _, b := range blocks {
			fmt.Printf("pushACK: sid: %d, blockID: %v\n", curSid, b)
			ackMaps.pushACK(curSid)
			curSid++
		}
	}
	/****汇聚*****/
	for i, blocks := range curRackNodes {
		nodeID := common.GetDataNodeIDFromIndex(rackID, i)
		//传输blocks到rootP
		for _, b := range blocks{
			fmt.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
				nodeID, common.GetNodeIP(nodeID), b, common.GetNodeIP(rootP))
			rangeLeft,rangeRight := getRangeFromBlockID(b)
			cmd := &config.CMD{
				SID: sid,
				BlockID: b,
				ToIPs: []string{common.GetNodeIP(rootP)},
				FromIP: common.GetNodeIP(nodeID),
				Helpers: make([]int, 0, 1),
				Matched: 0,
				SendSize: rangeRight-rangeLeft,
			}
			common.SendData(cmd, common.GetNodeIP(nodeID), config.NodeCMDListenPort, "")
			sid++
		}
	}

	sort.Ints(unionParities)
	fmt.Printf("DataUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
		stripe, parities, unionParities, curRackNodes)
}

func getRangeFromBlockID(blockID int) (rangeLeft,rangeRight int) {
	i := findBlockIndexInReqs(curDistinctReq, blockID)
	return curDistinctReq[i].RangeLeft, curDistinctReq[i].RangeRight
}

func parityUpdate1(rackID int, stripe []int) {
	curRackNodes := make([][]int, config.RackSize)
	parities := make([][]int, config.M * config.W)
	for _, blockID := range stripe {
		nodeID := common.GetNodeID(blockID)
		if byte(rackID) != getRackIDFromNodeID(byte(nodeID)) {
			continue
		}
		curRackNodes[nodeID-rackID*config.RackSize] = append(curRackNodes[nodeID-rackID*config.RackSize], blockID)
		for _, p := range common.RelatedParities(blockID){
			if arrays.Contains(parities[p], blockID) < 0 {
				parities[p] = append(parities[p], blockID)
			}
		}
	}

	unionParities := make([]int, 0, config.K * config.W)
	for _, p := range parities {
		unionParities = common.Union(p, unionParities)
	}
	if len(unionParities) == 0 {
		return
	}
	//选择一个rootD
	rootD := GetRootDataNodeID(curRackNodes, rackID)
	if rootD < 0 {
		log.Fatal("找不到rootParity")
		return
	}
	curSid := sid

	/****记录ack*****/
	parityNodeBlocks := GetParityNodeBlocks(parities)

	fmt.Printf("PataUpdate: parityNodeBlocks: %v\n", parityNodeBlocks)
	for _, blocks := range parityNodeBlocks {
		if len(blocks) == 0{
			continue
		}
		fmt.Printf("pushACK: sid: %d, blockID: %v\n", curSid, blocks)
		ackMaps.pushACK(curSid)
		curSid++
	}
	/****分发*****/
	for i, blocks := range parityNodeBlocks {
		if len(blocks) == 0{
			continue
		}
		parityID := i + config.K
		helpers := make([]int, 0, len(blocks))
		for _, b := range blocks {
			if common.GetNodeID(b) != rootD{
				helpers = append(helpers, b)
			}
		}
		//rangeLeft,rangeRight := getRangeFromBlockID(blocks[0])
		cmd := &config.CMD{
			SID: sid,
			BlockID: blocks[0],
			ToIPs: []string{common.GetNodeIP(parityID)},
			FromIP: common.GetNodeIP(rootD),
			Helpers: helpers,
			Matched: 0,
			SendSize: config.BlockSize,
		}
		common.SendData(cmd, common.GetNodeIP(rootD), config.NodeCMDListenPort, "")
		sid++
	}

	/****记录ack*****/
	for i, blocks := range curRackNodes {
		curID := rackID*config.RackSize + i
		if curID != rootD {
			for _, b := range blocks {
				fmt.Printf("pushACK: sid: %d, blockID: %v\n", curSid, b)
				ackMaps.pushACK(curSid)
				curSid++
			}
		}
	}
	/****汇聚*****/
	for i, blocks := range curRackNodes {
		curID := rackID*config.RackSize + i
		if curID != rootD {
			//传输blocks到rootD
			for _, b := range blocks{
				rangeLeft,rangeRight := getRangeFromBlockID(b)
				cmd := &config.CMD{
					SID: sid,
					BlockID: b,
					ToIPs: []string{common.GetNodeIP(rootD)},
					FromIP: common.GetNodeIP(curID),
					Helpers: make([]int, 0, 1),
					Matched: 0,
					SendSize: rangeRight-rangeLeft,
				}
				common.SendData(cmd, common.GetNodeIP(curID), config.NodeCMDListenPort, "")

				sid++
			}
		}
	}

	sort.Ints(unionParities)
	fmt.Printf("ParityUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
											stripe, parities, unionParities, curRackNodes)
}
func (p TAR_CAU) HandleCMD(cmd *config.CMD)  {
	//handleOneCMD(cmd)
	if len(cmd.Helpers) == 0 {	//本地数据，直接发送
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		//fmt.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)
		sendSizeRate := float32(len(buff)*1.0) / float32(config.BlockSize)
		fmt.Printf("读取 block:%d size:%.4f 本地数据.\n", cmd.BlockID, sendSizeRate)

		for _, toIP := range cmd.ToIPs {
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff: buff,
				FromIP: cmd.FromIP,
				ToIP: toIP,
				SID: cmd.SID,
				SendSize: cmd.SendSize,
			}
			sendSizeRate := float32(td.SendSize*1.0) / float32(config.BlockSize)
			fmt.Printf("发送 block:%d sendSize:%f 的数据到%s.\n", td.BlockID, sendSizeRate, toIP)
			common.SendData(td, toIP, config.NodeTDListenPort, "")
		}
	}else if !curReceivedTDs.isEmpty() {  //如果已收到过相关td
		CMDList.pushCMD(cmd)
		for _, td := range curReceivedTDs.getTDs(){
			handleWaitingCMDs(td)
		}
	}else{  //否则
		CMDList.pushCMD(cmd)
	}
}

func (p TAR_CAU) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
	//fmt.Printf("当前剩余ack：%d\n", ackMaps)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //ms检查是否全部完成，若完成，进入下一轮
			fmt.Printf("当前任务已完成...\n")
			IsRunning = false
		}
	}
}
func (p TAR_CAU) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	round = 0
	curReceivedTDs = &ReceivedTDs{
		TDs: make([]*config.TD, 0, config.MaxBatchSize),
	}
}

func (p TAR_CAU) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p TAR_CAU) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}


func (p TAR_CAU) GetActualBlocks() int {
	return actualBlocks
}

