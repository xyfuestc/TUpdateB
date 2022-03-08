package schedule

import (
	"EC/common"
	"EC/config"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"

)
type DXR_DU_Multi struct {

}

func (p DXR_DU_Multi) Init()  {
	totalCrossRackTraffic = 0
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
func (p DXR_DU_Multi) HandleTD(td *config.TD) {
	//记录当前轮次接收到的blockID
	curReceivedTDs.pushTD(td)
	//校验节点本地数据更新
	localID := arrays.ContainsString(config.NodeIPs, common.GetLocalIP())
	if localID >= config.K {
		common.WriteDeltaBlock(td.BlockID, td.Buff)
	}
	//返回ack
	ack := &config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	ReturnACK(ack)

	handleWaitingCMDs(td)
}



func (p DXR_DU_Multi) HandleReq(reqs []*config.ReqData)  {
	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		lenOfBatch := findDistinctReqs()
		//执行cau
		actualBlocks += len(curDistinctReq)
		//log.Printf("第%d轮 TAR-CAU：处理%d个block，剩余%v个block待处理。\n", round, len(curDistinctReq), len(totalReqs))
		log.Printf("第%d轮 DXR-DU-Multi：获取%d个请求，实际处理%d个block，剩余%v个block待处理。\n", round, lenOfBatch, len(curDistinctReq), len(totalReqs))

		dxr_du_multi()

		for IsRunning {
			
		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}
	//p.Clear()

}


func dxr_du_multi() {
	stripes := turnReqsToStripes()
	for _, stripe := range stripes{

		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {

				//统一同一stripe的rangeL和rangeR
				alignRangeOfStripe(stripe)

				if compareRacks(i, ParityRackIndex, stripe) {
					dxr_du_multi_parityUpdate(i, stripe)
				}else{
					dxr_du_multi_dataUpdate(i, stripe)
				}
			}
		}
	}
}


func dxr_du_multi_dataUpdate(rackID int, stripe []int)  {
	curRackNodes := make([][]int, config.RackSize)
	parities := make([][]int, config.M * config.W)
	for _, blockID := range stripe{
		nodeID := common.GetNodeID(blockID)
		if byte(rackID) != getRackIDFromNodeID(byte(nodeID)) {
			continue
		}
		//log.Printf("blockID: %d, nodeID: %d, rackID: %d\n", blockID, nodeID, rackID)
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
			log.Printf("pushACK: sid: %d, blockID: %v\n", curSid, b)
			ackMaps.pushACK(curSid)
			curSid++
		}
	}
	/****分发*****/
	log.Printf("DataUpdate: parityNodeBlocks: %v\n", parityNodeBlocks)
	for i, blocks := range parityNodeBlocks {
		parityID := i + config.K
		if parityID != rootP {
			//传输blocks到rootD
			for _, b := range blocks{
				//省略了合并操作，直接只发一条
				log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
					rootP, common.GetNodeIP(rootP), b, common.GetNodeIP(parityID))
				_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(b)
				//cmd := &config.CMD{
				//	SID: sid,
				//	BlockID: b,
				//	ToIPs: []string{common.GetNodeIP(parityID)},
				//	FromIP: common.GetNodeIP(rootP),
				//	Helpers: blocks,
				//	Matched: 0,
				//	SendSize: rangeRight - rangeLeft,
				//	//SendSize: config.BlockSize,
				//}
				//common.SendData(cmd, common.GetNodeIP(rootP), config.NodeCMDListenPort, "")

				cmd := config.CMDBufferPool.Get().(*config.CMD)
				cmd.SID = sid
				cmd.BlockID = b
				cmd.ToIPs = []string{common.GetNodeIP(parityID)}
				cmd.FromIP = common.GetNodeIP(rootP)
				cmd.Helpers = blocks
				cmd.Matched = 0
				cmd.SendSize = rangeRight - rangeLeft

				common.SendData(cmd, common.GetNodeIP(rootP), config.NodeCMDListenPort)

				config.CMDBufferPool.Put(cmd)

				sid++
				break
			}
		}
	}

	for _, blocks := range curRackNodes {
		//传输blocks到rootP
		for _, b := range blocks {
			log.Printf("pushACK: sid: %d, blockID: %v\n", curSid, b)
			ackMaps.pushACK(curSid)
			curSid++
		}
	}
	/****汇聚*****/
	for i, blocks := range curRackNodes {
		nodeID := common.GetDataNodeIDFromIndex(rackID, i)
		//传输blocks到rootP
		for _, b := range blocks {
			log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
				nodeID, common.GetNodeIP(nodeID), b, common.GetNodeIP(rootP))
			_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(b)
			//cmd := &config.CMD{
			//	SID: sid,
			//	BlockID: b,
			//	ToIPs: []string{common.GetNodeIP(rootP)},
			//	FromIP: common.GetNodeIP(nodeID),
			//	Helpers: make([]int, 0, 1),
			//	Matched: 0,
			//	SendSize: rangeRight - rangeLeft,
			//}
			//common.SendData(cmd, common.GetNodeIP(nodeID), config.NodeCMDListenPort, "")

			cmd := config.CMDBufferPool.Get().(*config.CMD)
			cmd.SID = sid
			cmd.BlockID = b
			cmd.ToIPs = []string{common.GetNodeIP(rootP)}
			cmd.FromIP = common.GetNodeIP(nodeID)
			cmd.Helpers = make([]int, 0, 1)
			cmd.Matched = 0
			cmd.SendSize = rangeRight - rangeLeft

			common.SendData(cmd, common.GetNodeIP(nodeID), config.NodeCMDListenPort)

			config.CMDBufferPool.Put(cmd)

			sid++
			//统计跨域流量
			totalCrossRackTraffic += rangeRight - rangeLeft
		}
	}

	sort.Ints(unionParities)
	log.Printf("DataUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
		stripe, parities, unionParities, curRackNodes)
}


func dxr_du_multi_parityUpdate(rackID int, stripe []int) {

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

	log.Printf("PataUpdate: parityNodeBlocks: %v\n", parityNodeBlocks)
	for _, blocks := range parityNodeBlocks {
		if len(blocks) == 0{
			continue
		}
		log.Printf("pushACK: sid: %d, blockID: %v\n", curSid, blocks)
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
		_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(blocks[0])
		//cmd := &config.CMD{
		//	SID: sid,
		//	BlockID: blocks[0],
		//	ToIPs: []string{common.GetNodeIP(parityID)},
		//	FromIP: common.GetNodeIP(rootD),
		//	Helpers: helpers,
		//	Matched: 0,
		//	SendSize: rangeRight - rangeLeft,
		//}
		cmd := config.CMDBufferPool.Get().(*config.CMD)
		cmd.SID = sid
		cmd.BlockID = blocks[0]
		cmd.ToIPs = []string{common.GetNodeIP(parityID)}
		cmd.FromIP = common.GetNodeIP(rootD)
		cmd.Helpers = helpers
		cmd.Matched = 0
		cmd.SendSize = rangeRight - rangeLeft

		common.SendData(cmd, common.GetNodeIP(rootD), config.NodeCMDListenPort)

		config.CMDBufferPool.Put(cmd)

		sid++
		//统计跨域流量
		totalCrossRackTraffic += rangeRight - rangeLeft
	}

	/****记录ack*****/
	for i, blocks := range curRackNodes {
		curID := rackID*config.RackSize + i
		if curID != rootD {
			for _, b := range blocks {
				log.Printf("pushACK: sid: %d, blockID: %v\n", curSid, b)
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
				_, rangeLeft,rangeRight := getBlockRangeFromDistinctReqs(b)
				//cmd := &config.CMD{
				//	SID: sid,
				//	BlockID: b,
				//	ToIPs: []string{common.GetNodeIP(rootD)},
				//	FromIP: common.GetNodeIP(curID),
				//	Helpers: make([]int, 0, 1),
				//	Matched: 0,
				//	SendSize: rangeRight - rangeLeft,
				//}
				//common.SendData(cmd, common.GetNodeIP(curID), config.NodeCMDListenPort, "")

				cmd := config.CMDBufferPool.Get().(*config.CMD)
				cmd.SID = sid
				cmd.BlockID = b
				cmd.ToIPs = []string{common.GetNodeIP(rootD)}
				cmd.FromIP = common.GetNodeIP(rootD)
				cmd.Helpers = make([]int, 0, 1)
				cmd.Matched = 0
				cmd.SendSize = rangeRight - rangeLeft

				common.SendData(cmd, common.GetNodeIP(curID), config.NodeCMDListenPort)

				config.CMDBufferPool.Put(cmd)

				sid++
			}
		}
	}

	sort.Ints(unionParities)
	log.Printf("ParityUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
											stripe, parities, unionParities, curRackNodes)
}
func (p DXR_DU_Multi) HandleCMD(cmd *config.CMD)  {
	//handleOneCMD(cmd)
	if len(cmd.Helpers) == 0 {	//本地数据，直接发送
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		//log.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)
		sendSizeRate := float32(len(buff)*1.0) / float32(config.BlockSize) * 100.0
		log.Printf("读取 block:%d size:%.2f%% 本地数据.\n", cmd.BlockID, sendSizeRate)

		for _, toIP := range cmd.ToIPs {
			//td := &config.TD{
			//	BlockID: cmd.BlockID,
			//	Buff: buff,
			//	FromIP: cmd.FromIP,
			//	ToIP: toIP,
			//	SID: cmd.SID,
			//	SendSize: cmd.SendSize,
			//}
			td := config.TDBufferPool.Get().(*config.TD)
			td.BlockID = cmd.BlockID
			td.Buff = buff[:cmd.SendSize]
			td.FromIP = cmd.FromIP
			td.ToIP = toIP
			td.SID = cmd.SID
			td.SendSize = cmd.SendSize
			sendSizeRate := float32(td.SendSize*1.0) / float32(config.BlockSize) * 100.0
			log.Printf("发送 block:%d sendSize:%.2f%% 的数据到%s.\n", td.BlockID, sendSizeRate, toIP)
			common.SendData(td, toIP, config.NodeTDListenPort)

			config.TDBufferPool.Put(td)
		}

		config.BlockBufferPool.Put(buff)

	}else if !curReceivedTDs.isEmpty() {  //如果已收到过相关td
		CMDList.pushCMD(cmd)
		for _, td := range curReceivedTDs.getTDs(){
			handleWaitingCMDs(td)
		}
	}else{  //否则
		CMDList.pushCMD(cmd)
	}
}

func (p DXR_DU_Multi) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
	//log.Printf("当前剩余ack：%d\n", ackMaps)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //ms检查是否全部完成，若完成，进入下一轮
			log.Printf("当前任务已完成...\n")
			IsRunning = false
		}
	}
}
func (p DXR_DU_Multi) Clear()  {
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

	curReceivedTDs = &ReceivedTDs{
		TDs: make([]*config.TD, 0, config.MaxBatchSize),
	}
}

func (p DXR_DU_Multi) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p DXR_DU_Multi) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}


func (p DXR_DU_Multi) GetActualBlocks() int {
	return actualBlocks
}

