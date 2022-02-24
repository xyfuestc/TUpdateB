package schedule

import (
	"EC/common"
	"EC/config"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"
)
type CAU struct {
	Base
}
const ParityRackIndex = config.RackSize - 1
var round = 0
var IsRunning = true   //标志是否进入下一轮迭代
var totalReqs = make([]*config.ReqData, config.MaxBatchSize, config.MaxBatchSize)
var curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
var actualBlocks = 0
func (p CAU) Init()  {
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
}

func (p CAU) HandleTD(td *config.TD) {
	//校验节点本地数据更新
	localID := arrays.ContainsString(config.NodeIPs, common.GetLocalIP())
	log.Printf("cau localID:%d\n", localID)
	if localID >= config.K {
		go common.WriteDeltaBlock(td.BlockID, td.Buff)
	}
	//返回ack
	ack := &config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	ReturnACK(ack)
	//有等待任务
	handleWaitingCMDs(td)
}

func findDistinctBlocks() []*config.ReqData {
	curMatchReqs := make([]*config.ReqData, 0, config.MaxBatchSize)
	if len(totalReqs) > config.MaxBatchSize {
		curMatchReqs = totalReqs[:config.MaxBatchSize]
		for _, req := range curMatchReqs {
			if arrays.Contains(curDistinctBlocks, req.BlockID) < 0 {
				curDistinctBlocks = append(curDistinctBlocks, req.BlockID)
			}
		}
		totalReqs = totalReqs[config.MaxBatchSize:]
	}else { //处理最后不到100个请求
		curMatchReqs = totalReqs
		for _, req := range curMatchReqs {
			if arrays.Contains(curDistinctBlocks, req.BlockID) < 0 {
				curDistinctBlocks = append(curDistinctBlocks, req.BlockID)
			}
		}
		totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
	}
	return curMatchReqs
}
func (p CAU) HandleReq(reqs []*config.ReqData)  {
	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))
	for len(totalReqs) > 0 {
		//过滤blocks
		curMatchBlocks := findDistinctBlocks()
		//执行cau
		actualBlocks += len(curDistinctBlocks)
		//log.Printf("第%d轮 CAU：处理%d个block\n", round, len(curDistinctBlocks))
		log.Printf("第%d轮 CAU：获取%d个请求，实际处理%d个block\n", round, len(curMatchBlocks), len(curDistinctBlocks))

		p.cau()

		for IsRunning {
			
		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}
}

func (p CAU) cau() {
	stripes := turnBlocksToStripes()
	for _, stripe := range stripes{
		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {
				if compareRacks(i, ParityRackIndex, stripe) {
					parityUpdate(i, stripe)
				}else{
					dataUpdate(i, stripe)
				}
			}
		}
	}
}
func dataUpdate(rackID int, stripe []int)  {
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

func parityUpdate(rackID int, stripe []int) {
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
		//统计跨域流量
		totalCrossRackTraffic += config.BlockSize
		sid++
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

func GetParityNodeBlocks(parities [][]int) [][]int {
	parityNodeBlocks := make([][]int, config.M)
	for i, blocks := range parities {
		parityIndex := i / config.W
		for _, b := range blocks {
			if arrays.Contains(parityNodeBlocks[parityIndex], b) < 0 {
				parityNodeBlocks[parityIndex] = append(parityNodeBlocks[parityIndex], b)
			}
		}
	}
	for i := 0; i < len(parityNodeBlocks); i++ {
		sort.Ints(parityNodeBlocks[i])
	}
	return parityNodeBlocks
}

func GetRootDataNodeID(blocksOfNodes [][]int, rackID int) int {
	for i, blocksOfNode := range blocksOfNodes {
		if len(blocksOfNode) > 0{
			return i + rackID * config.RackSize
		}
	}
	return -1
}

func compareRacks(rackIndexI, rackIndexJ int, stripe []int) bool {
	if getRackStripeNum(rackIndexI, stripe) >
		getRackStripeNum(rackIndexJ, stripe) {
		return true
	}else{
		return false
	}
}

/*统计当前rack更新量*/
func getRackStripeNum(curRackID int, blocksOfStripe []int) int  {
	//统计parityRack更新量
	if curRackID == ParityRackIndex {
		return getParityUpdateNums(blocksOfStripe)
	}

	curRackNodeIDs := make([]int, 0, config.RackSize)
	for _, b := range blocksOfStripe {
		rackID := getRackIDFromBlockID(b)
		if rackID == byte(curRackID) {
			nodeID := common.GetNodeID(b)
			if arrays.Contains(curRackNodeIDs, nodeID) < 0 {
				curRackNodeIDs = append(curRackNodeIDs, nodeID)
			}
		}
	}
	return len(curRackNodeIDs)
}
//统计当前curRackID中有多少数据更新（某一条带）
func getRackUpdateNums(curRackID int, curBlocksOfStripe []int) int  {
	rackUpdateNums := make([]int, 0, len(curBlocksOfStripe))
	for _, block := range curBlocksOfStripe {
		curBlockRackID := getRackIDFromBlockID(block)
		if curBlockRackID == byte(curRackID) {
			rackUpdateNums = append(rackUpdateNums, block)
		}
	}
	return len(rackUpdateNums)
}
func getRackIDFromBlockID(blockID int) byte {
	nodeID := common.GetNodeID(blockID)
	return getRackIDFromNodeID(byte(nodeID))
}

func getRackIDFromNodeID(nodeID byte) byte  {
	return nodeID / byte(config.RackSize)
}
/*统计当前需要更新的块与哪些parityNode有关*/
func getParityUpdateNums(blocksOfStripe []int) int {
	parityNodeIDs := make([]int, 0, config.M)
	for _, b := range blocksOfStripe {
		curParityIDs := common.RelatedParities(b) // eg: [0 2 4 6 8 9]
		curParityNodeIDs := common.RelatedParityNodes(curParityIDs)
		for _, id := range curParityNodeIDs {
			if arrays.Contains(parityNodeIDs, int(id)) < 0 {
				parityNodeIDs = append(parityNodeIDs, int(id))
			}
		}
	}
	return len(parityNodeIDs)
}

func turnBlocksToStripes() map[int][]int {
	stripes := map[int][]int{}
	for _, block := range curDistinctBlocks {
		stripeID := common.GetRSStripeIDFromBlockID(block)
		stripes[stripeID] = append(stripes[stripeID], block)
	}
	return  stripes
}

func (p CAU) HandleCMD(cmd *config.CMD)  {
	//handleOneCMD(cmd)
	if len(cmd.Helpers) == 0 {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)

		for _, toIP := range cmd.ToIPs {
			//td := &config.TD{
			//	BlockID: cmd.BlockID,
			//	Buff: buff[:cmd.SendSize],
			//	FromIP: cmd.FromIP,
			//	ToIP: toIP,
			//	SID: cmd.SID,
			//}

			td := config.TDBufferPool.Get().(*config.TD)
			td.BlockID = cmd.BlockID
			td.Buff = buff[:cmd.SendSize]
			td.FromIP = cmd.FromIP
			td.ToIP = toIP
			td.SID = cmd.SID
			common.SendData(td, toIP, config.NodeTDListenPort, "")
			config.TDBufferPool.Put(td)
			//common.SendData(td, toIP, config.NodeTDListenPort, "")
		}

		config.BlockBufferPool.Put(buff)
	}else{
		CMDList.pushCMD(cmd)
	}
}

func (p CAU) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
	//log.Printf("当前剩余ack：%d\n", ackMaps)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //ms检查是否全部完成，若完成，进入下一轮
			IsRunning = false
		}
	}
}
func (p CAU) Clear()  {
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

func (p CAU) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func GetRootParityID(parities [][]int) int {
	for i, parity := range parities {
		if len(parity) > 0 {
			rootP := common.GetParityIDFromIndex(i)
			return rootP
		}
	}
	return -1
}
func (p CAU) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p CAU) GetActualBlocks() int {
	return actualBlocks
}

