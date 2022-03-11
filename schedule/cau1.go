package schedule

import (
	"EC/common"
	"EC/config"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"
)

type CAU1 struct {
	Base
}
func (p CAU1) Init()  {
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
	actualBlocks = 0
	round = 0
	sid = 0
}

func (p CAU1) HandleTD(td *config.TD) {
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
	//处理需要helpers的CMD
	handleWaitingCMDs(td)
}

func (p CAU1) HandleReq(reqs []*config.ReqData)  {
	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))
	for len(totalReqs) > 0 {
		//获取curDistinctBlocks
		curMatchBlocks := findDistinctBlocks()
		//执行cau
		actualBlocks += len(curDistinctBlocks)
		log.Printf("第%d轮 CAU1：获取%d个请求，实际处理%d个block\n", round, len(curMatchBlocks), len(curDistinctBlocks))

		p.cau1()

		for IsRunning {

		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}
}
func (p CAU1) cau1() {
	stripes := turnBlocksToStripes()
	for _, stripe := range stripes{
		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {
				//如果当前dataRack的更新量 > 需要更新的ParityIDs
				if getRackUpdateNums(i, stripe) > getParityUpdateNums(stripe) {
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
	/****记录ack*****/
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
				common.SendCMDWithHelpers(common.GetNodeIP(rootP), []string{common.GetNodeIP(parityID)},
					sid, b, blocks)
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
		for _, b := range blocks{
			log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
				nodeID, common.GetNodeIP(nodeID), b, common.GetNodeIP(rootP))
			common.SendCMD(common.GetNodeIP(nodeID), []string{common.GetNodeIP(rootP)}, sid, b)
			sid++
			//统计跨域流量
			totalCrossRackTraffic += config.BlockSize
		}
	}
	sort.Ints(unionParities)
	log.Printf("DataUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
		stripe, parities, unionParities, curRackNodes)
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
		common.SendCMDWithHelpers(common.GetNodeIP(rootD), []string{common.GetNodeIP(parityID)},
			sid, blocks[0], helpers)
		sid++
		//统计跨域流量
		totalCrossRackTraffic += config.BlockSize
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
				common.SendCMD(common.GetNodeIP(curID), []string{common.GetNodeIP(rootD)}, sid, b)
				sid++
			}
		}
	}

	sort.Ints(unionParities)
	log.Printf("ParityUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
		stripe, parities, unionParities, curRackNodes)
}

func (p CAU1) HandleCMD(cmd *config.CMD)  {
	//handleOneCMD(cmd)
	if len(cmd.Helpers) == 0 {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		//log.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)

		for _, toIP := range cmd.ToIPs {
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff: buff,
				FromIP: cmd.FromIP,
				ToIP: toIP,
				SID: cmd.SID,
				SendSize: cmd.SendSize,
			}
			//
			//common.SendData(td, toIP, config.NodeTDListenPort, "")

			//td := config.TDBufferPool.Get().(*config.TD)
			//td.BlockID = cmd.BlockID
			//td.Buff = buff
			//td.FromIP = cmd.FromIP
			//td.ToIP = toIP
			//td.SID = cmd.SID
			//td.SendSize = cmd.SendSize
			//sendSizeRate := float32(td.SendSize*1.0) / float32(config.BlockSize) * 100.0
			//log.Printf("发送 block:%d sendSize:%.2f%% 的数据到%s.\n", td.BlockID, sendSizeRate, toIP)
			common.SendData(td, toIP, config.NodeTDListenPort)

			//config.TDBufferPool.Put(td)
		}
		config.BlockBufferPool.Put(buff)
	}else{
		CMDList.pushCMD(cmd)
	}
}

func (p CAU1) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//SentMsgLog.popMsg(ack.SID)      //该SID不需要重发
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			IsRunning = false
		}
	}
}
func (p CAU1) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
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
}

func (p CAU1) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p CAU1) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p CAU1) GetActualBlocks() int {
	return actualBlocks
}


