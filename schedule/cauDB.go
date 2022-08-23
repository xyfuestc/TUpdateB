package schedule

import (
	"EC/common"
	"EC/config"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"
	"sync"
)
type CAU_DB struct {

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
func (p CAU_DB) Init()  {

	totalCrossRackTraffic = 0
	actualBlocks = 0
	round = 0
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
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)

	curReceivedTDs = &ReceivedTDs{
		TDs: make([]*config.TD, 0, config.MaxBatchSize),
	}

	ClearChannels()
}
func (p CAU_DB) HandleTD(td *config.TD) {

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

	//处理需要helpers的cmd
	handleWaitingCMDs(td)
}

func handleWaitingCMDs(td *config.TD) {
	//有等待任务
	indexes := meetCMDNeed(td.BlockID)
	if len(indexes) > 0 {
		for _, i := range indexes {
			cmd := i
			log.Printf("执行TD任务：sid:%d blockID:%d\n", cmd.SID, cmd.BlockID)
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		for _, i := range indexes {
			cmd := i
			toIP := cmd.ToIPs[0]
			//td := &config.TD{
			//	BlockID: cmd.BlockID,
			//	Buff:    td.Buff,
			//	FromIP:  cmd.FromIP,
			//	ToIP:    toIP,
			//	SID:     cmd.SID,
			//	SendSize: cmd.SendSize,
			//}
			sendTD := config.TDBufferPool.Get().(*config.TD)
			sendTD.BlockID = cmd.BlockID
			sendTD.Buff = td.Buff
			sendTD.FromIP = cmd.FromIP
			sendTD.ToIP = toIP
			sendTD.SID = cmd.SID
			sendTD.SendSize = cmd.SendSize
			log.Printf("tar-cau handleWaitingCMDs: sendTD.SendSize: %+v,len(td.Buff): %+v \n", sendTD.SendSize, len(td.Buff))
			sendSizeRate := float32(sendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
			log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", td.BlockID, sendSizeRate, toIP)
			common.SendData(sendTD, toIP, config.NodeTDListenPort)

			config.TDBufferPool.Put(sendTD)
		}
	}
}

func (p CAU_DB) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		lenOfBatch := findDistinctReqs()

		actualBlocks += len(curDistinctReq)
		log.Printf("第%d轮 DXR-DU：获取%d个请求，实际处理%d个block，剩余%v个block待处理。\n", round, lenOfBatch, len(curDistinctReq), len(totalReqs))

		//执行reqs
		cau_db()

		select {
		case <-Done:
			log.Printf("本轮结束！\n")
			log.Printf("======================================\n")
			round++
			p.Clear()
		}
	}
}

func turnMatchReqsToDistinctReqs(curMatchReqs []*config.ReqData)   {

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
func findDistinctReqs() int {
	curMatchReqs := make([]*config.ReqData, 0, config.MaxBatchSize)
	if len(totalReqs) > config.MaxBatchSize {
		curMatchReqs = totalReqs[:config.MaxBatchSize]
		totalReqs = totalReqs[config.MaxBatchSize:]
	}else { //处理最后不到100个请求
		curMatchReqs = totalReqs
		totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
	}
	//将同一block的不同修改合并（rangL，rangR）
	turnMatchReqsToDistinctReqs(curMatchReqs)

	return len(curMatchReqs)
}

func findBlockIndexInReqs(reqs []*config.ReqData, blockID int) int {
	for i, req := range reqs {
		if req.BlockID == blockID {
			return i
		}
	}
	return -1
}
func turnReqsToStripes(reqs []*config.ReqData) map[int][]int {
	stripes := map[int][]int{}
	for _, req := range reqs {
		stripeID := common.GetStripeIDFromBlockID(req.BlockID)
		stripes[stripeID] = append(stripes[stripeID], req.BlockID)
	}
	return  stripes
}

func cau_db() {
	stripes := turnReqsToStripes(curDistinctReq)
	for _, stripe := range stripes{

		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {

				//统一同一stripe的rangeL和rangeR
				alignRangeOfStripe(stripe)

				if compareRacks(i, ParityRackIndex, stripe) {
					dxr_parityUpdate(i, stripe)
				}else{
					dxr_dataUpdate(i, stripe)
				}
			}
		}
	}
}

func alignRangeOfStripe(stripe []int) {
	//1、寻找同一个stripe下最大范围的rangeL，rangeR
	minRangeL, maxRangeR := config.BlockSize, 0
	reqIndexList := make([]int, 0, len(stripe))
	//log.Printf("len(stripe) = %d", len(stripe))
	for _, b := range stripe{
		j, rangeL, rangeR := getBlockRangeFromDistinctReqs(b, curDistinctReq)
		//归一化
		rangeL = rangeL % config.BlockSize
		rangeR = rangeR % config.BlockSize
		//log.Printf("%d=(%d,%d)",j,rangeL,rangeR)
		if j == -1 {
			continue
		}
		if rangeL < minRangeL {
			minRangeL = rangeL
		}
		if rangeR > maxRangeR {
			maxRangeR = rangeR
		}
		//记录哪些block需要统一range
		reqIndexList = append(reqIndexList, j)
	}
	//log.Printf("len(reqIndexList) = %d", len(reqIndexList))
	for i := range reqIndexList{
		//log.Printf("需要设置curDistinctReq[%d].RangeLeft=%d,RangeRight=%d", i, minRangeL, maxRangeR)
		curDistinctReq[i].RangeLeft = minRangeL
		curDistinctReq[i].RangeRight = maxRangeR
	}
}
func dxr_dataUpdate(rackID int, stripe []int)  {
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
				_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(b, curDistinctReq)
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
			_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(b, curDistinctReq)
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

func getBlockRangeFromDistinctReqs(blockID int, reqs []*config.ReqData) (i, rangeLeft, rangeRight int) {
	j := findBlockIndexInReqs(reqs, blockID)
	return j, reqs[j].RangeLeft, reqs[j].RangeRight
}


func dxr_parityUpdate(rackID int, stripe []int) {

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
		_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(blocks[0], curDistinctReq)
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
				_, rangeLeft,rangeRight := getBlockRangeFromDistinctReqs(b, curDistinctReq)
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
func (p CAU_DB) HandleCMD(cmd *config.CMD)  {

	//直接发送（数据在本地）
	if len(cmd.Helpers) == 0 {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		//log.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)
		sendSizeRate := float32(len(buff)*1.0) / float32(config.BlockSize) * 100.0
		log.Printf("读取 block:%d size:%.2f%% 本地数据.\n", cmd.BlockID, sendSizeRate)

		for _, toIP := range cmd.ToIPs {
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff: buff,
				FromIP: cmd.FromIP,
				ToIP: toIP,
				SID: cmd.SID,
				SendSize: cmd.SendSize,
			}
			//td := config.TDBufferPool.Get().(*config.TD)
			//td.BlockID = cmd.BlockID
			//td.Buff = buff[:cmd.SendSize]
			//td.FromIP = cmd.FromIP
			//td.ToIP = toIP
			//td.SID = cmd.SID
			//td.SendSize = cmd.SendSize
			sendSizeRate := float32(td.SendSize*1.0) / float32(config.BlockSize) * 100.0
			log.Printf("发送 block:%d sendSize:%.2f%% 的数据到%s.\n", td.BlockID, sendSizeRate, toIP)
			common.SendData(td, toIP, config.NodeTDListenPort)

			//config.TDBufferPool.Put(td)
		}
		config.BlockBufferPool.Put(buff)

	//helpers已收到一部分
	}else if !curReceivedTDs.isEmpty() {
		CMDList.pushCMD(cmd)
		for _, td := range curReceivedTDs.getTDs(){
			handleWaitingCMDs(td)
		}

	//helpers未就位
	}else{
		CMDList.pushCMD(cmd)
	}
}

func (p CAU_DB) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //检查是否全部完成，若完成，进入下一轮
			Done <- true
		}
	}
}
func (p CAU_DB) Clear()  {
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

func (p CAU_DB) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p CAU_DB) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}


func (p CAU_DB) GetActualBlocks() int {
	return actualBlocks
}

func (p CAU_DB) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.MB
}
