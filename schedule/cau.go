package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"sort"
)
type CAU struct {
	Base
}

const ParityRackIndex = config.RackSize - 1

//var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var NumOfCurNeedUpdateBlocks = 0
var round = 0
var IsRunning = true   //check CAU is running or not
var totalBlocks = make([]int, config.MaxBatchSize, config.MaxBatchSize)
var NumOfBlocks = 0
var Now float32 = 0
var curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
var actualBlocks = 0
func (p CAU) Init()  {
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}

	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
}

func (p CAU) HandleTD(td *config.TD)  {
	handleOneTD(td)
}

func (p CAU) HandleReq(blocks []int)  {
	totalBlocks = blocks
	NumOfBlocks = len(totalBlocks)
	fmt.Printf("一共接收到%d个请求...\n", len(totalBlocks))
	curMatchBlocks := make([]int, 0, config.MaxBatchSize)
	for len(totalBlocks) > 0 {
		//获取curDistinctBlocks
		if len(totalBlocks) > config.MaxBatchSize {
			curMatchBlocks = totalBlocks[:config.MaxBatchSize]
			for _, b := range curMatchBlocks{
				if arrays.Contains(curDistinctBlocks, b) < 0 {
					curDistinctBlocks = append(curDistinctBlocks, b)
				}
			}
			totalBlocks = totalBlocks[config.MaxBatchSize:]
		}else { //处理最后不到100个请求
			curMatchBlocks = totalBlocks
			for _, b := range curMatchBlocks{
				if arrays.Contains(curDistinctBlocks, b) < 0 {
					curDistinctBlocks = append(curDistinctBlocks, b)
				}
			}
			totalBlocks = make([]int, 0, config.MaxBlockSize)
		}
		//执行cau
		actualBlocks += len(curDistinctBlocks)
		fmt.Printf("第%d轮 CAU：获取%d个请求，实际处理%d个block\n", round, len(curMatchBlocks), len(curDistinctBlocks))

		cau()

		for IsRunning {
			
		}
		round++
		p.Clear()
	}
	//p.Clear()

}

func cau() {
	fmt.Printf("curDistinctBlocks: %v", curDistinctBlocks)
	//for _, b := range curDistinctBlocks {
	//	ackMaps.pushACK(b)
	//}

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
func GetBlockColumn(blockID int) int {
	return blockID % (config.K * config.W)
}

func dataUpdate(rackID int, stripe []int)  {
	curRackNodes := make([][]int, config.RackSize)
	parities := make([][]int, config.M * config.W)
	for _, blockID := range stripe{
		nodeID := common.GetNodeID(blockID)
		if byte(rackID) != getRackIDFromNodeID(byte(nodeID)) {
			continue
		}
		fmt.Printf("blockID: %d, nodeID: %d, rackID: %d\n", blockID, nodeID, rackID)
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
	fmt.Println("rootP: ", rootP)


	/****记录ack*****/
	curSid := sid
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
			common.SendCMD(common.GetNodeIP(nodeID), []string{common.GetNodeIP(rootP)}, sid, b)
			sid++
		}
	}

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
				common.SendCMD(common.GetNodeIP(rootP), []string{common.GetNodeIP(parityID)}, sid, b)
				sid++
				break
			}
		}
	}
	sort.Ints(unionParities)
	fmt.Printf("DataUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
		stripe, parities, unionParities, curRackNodes)
}

func parityUpdate(rackID int, stripe []int) {
	curRackNodes := make([][]int, config.RackSize)
	parities := make([][]int, config.M * config.W)
	for _, blockID := range stripe{
		nodeID := common.GetNodeID(blockID)
		if byte(rackID) != getRackIDFromNodeID(byte(nodeID)) {
			continue
		}
		curRackNodes[nodeID-rackID*config.RackSize] = append(curRackNodes[nodeID-rackID*config.RackSize], blockID)
		for _, p := range common.RelatedParities(blockID){
			if arrays.Contains(parities[p], GetBlockColumn(blockID)) < 0 {
				parities[p] = append(parities[p], GetBlockColumn(blockID))
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
				common.SendCMD(common.GetNodeIP(curID), []string{common.GetNodeIP(rootD)}, sid, b)
				sid++
			}
		}
	}
	/****记录ack*****/
	parityNodeBlocks := GetParityNodeBlocks(parities)
	fmt.Printf("PataUpdate: parityNodeBlocks: %v\n", parityNodeBlocks)
	for _, b := range parityNodeBlocks {
		fmt.Printf("pushACK: sid: %d, blockID: %v\n", curSid, b)
		ackMaps.pushACK(curSid)
		curSid++
	}
	/****分发*****/
	for i, blocks := range parityNodeBlocks {
		parityID := i + config.K
		for _, b := range blocks {//省略了合并操作，直接只发一条
			common.SendCMD(common.GetNodeIP(rootD), []string{common.GetNodeIP(parityID)}, sid, b)
			sid++
			break
		}
	}
	sort.Ints(unionParities)
	fmt.Printf("ParityUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
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

//blocksIDs没有重复元素
func getRackStripeNum(index int, blocks []int) int  {
	if index == ParityRackIndex {
		return getParityRackUpdateNum(blocks)
	}
	curRackNodeIDs := make([]byte, 0, config.K / config.RackSize)
	for _, b := range blocks {
		rackID := getRackIDFromBlockID(b)
		if rackID == byte(index) {
			nodeID := common.GetNodeID(b)
			if arrays.Contains(curRackNodeIDs, nodeID) < 0 {
				curRackNodeIDs = append(curRackNodeIDs, byte(nodeID))
			}
		}
	}
	return len(curRackNodeIDs)
}
func getRackIDFromBlockID(blockID int) byte {
	nodeID := common.GetNodeID(blockID)
	return getRackIDFromNodeID(byte(nodeID))
}

func getRackIDFromNodeID(nodeID byte) byte  {
	return nodeID / byte(config.RackSize)
}

func getParityRackUpdateNum(blocks []int) int {
	parityIDs := make([]byte, 0, config.M)
	for _, b := range blocks {
		parities := common.RelatedParities(b)
		parityNodeIDs := common.RelatedParityNodes(parities)
		for _, id := range parityNodeIDs  {
			if arrays.Contains(parityIDs, id) < 0 {
				parityIDs = append(parityIDs, id)
			}
		}
	}
	return len(parityIDs)
}

func turnBlocksToStripes() map[int][]int {
	stripes := map[int][]int{}
	for _, b := range curDistinctBlocks{
		stripeID := common.GetStripeIDFromBlockID(b)
		stripes[stripeID] = append(stripes[stripeID], b)
	}
	return  stripes
}

func (p CAU) HandleCMD(cmd *config.CMD)  {
	handlOneCMD(cmd)
}

func (p CAU) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
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
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
}

func (p CAU) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func GetRootParityID(parities [][]int) int {
	for i, parity := range parities {
		if len(parity) > 0{
			return common.GetParityIDFromIndex(i)
		}
	}
	return -1
}
func (p CAU) IsFinished() bool {
	return len(totalBlocks) == 0 && ackMaps.isEmpty()
}
func GetActualBlocks() int {
	return actualBlocks
}

