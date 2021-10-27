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
var IsRunning = false   //check CAU is running or not
const maxBatchSize = 100
var totalBlocks = make([]int, config.MaxBatchSize, config.MaxBatchSize)
var NumOfBlocks = 0
var Now float32 = 0
var curDistinctBlocks = make([]int, 0, 100)

func (p CAU) Init()  {

}

func (p CAU) HandleTD(td *config.TD)  {
	handleOneTD(td)
}

func (p CAU) HandleReq(blocks []int)  {
	totalBlocks = blocks
	NumOfBlocks = len(totalBlocks)
	fmt.Printf("一共接收到%d个请求...\n", len(totalBlocks))
	curMatchBlocks := make([]int, 0, 100)
	for len(totalBlocks) > 0 {
		//获取curDistinctBlocks
		if len(totalBlocks) > 100 {
			curMatchBlocks = totalBlocks[:100]
			for _, b := range curMatchBlocks{
				if arrays.Contains(curDistinctBlocks, b) < 0 {
					curDistinctBlocks = append(curDistinctBlocks, b)
				}
			}
			totalBlocks = totalBlocks[100:]
			//fmt.Printf("剩余请求数量：%d\n",len(totalBlocks))

		}else {
			curMatchBlocks = totalBlocks
			//fmt.Printf("最后处理%d个请求...\n", len(curMatchBlocks))
			for _, b := range curMatchBlocks{
				if arrays.Contains(curDistinctBlocks, b) < 0 {
					curDistinctBlocks = append(curDistinctBlocks, b)
				}
			}
			totalBlocks = make([]int, 0, 1000000)
		}
		//执行cau
		fmt.Printf("第%d轮 CAU：获取%d个请求，实际处理%d个block\n", round, len(curMatchBlocks), len(curDistinctBlocks))

		cau()
		round++

		p.Clear()
	}

	sumTime := float32(Now)*1.0/1000
	averageOneUpdateSpeed := sumTime / float32(NumOfBlocks)
	throughput :=  float32(NumOfBlocks) * ( float32(config.BlockSize) / config.Megabyte) / sumTime

	fmt.Printf("CAU 总耗时: %0.2fs, 完成更新任务: %d, 单个更新速度: %0.4fs, 吞吐量: %0.2f个/s",
		sumTime, NumOfBlocks, averageOneUpdateSpeed, throughput)

	p.Clear()

}

func cau() {
	fmt.Println(curDistinctBlocks)
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
	curRackNodes := make([][]int, config.N / config.RackSize)
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

	//选择一个rootP
	rootP := GetRootParityID(parities)
	if rootP < 0 {
		log.Fatal("找不到rootParity")
		return
	}
	fmt.Println("rootP: ", rootP)

	/****汇聚*****/
	for i, blocks := range curRackNodes {
		curID := common.GetDataNodeIDFromIndex(rackID, i)
		//传输blocks到rootD
		for _, b := range blocks{
			common.SendCMD(common.GetNodeIP(curID), []string{common.GetNodeIP(rootP)}, b, b)
		}
	}

	/****分发*****/
	for i, blocks := range parities{
		pID := common.GetParityIDFromIndex(i)
		if pID != rootP {
			//传输blocks到rootD
			for _, b := range blocks{
				common.SendCMD(common.GetNodeIP(rootP), []string{common.GetNodeIP(pID)}, b, b)
			}
		}
	}
	sort.Ints(unionParities)
	fmt.Printf("ParityUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
		stripe, parities, unionParities, curRackNodes)
}

func parityUpdate(rackID int, stripe []int) {
	curRackNodes := make([][]int, config.N / config.RackSize)
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
	/****汇聚*****/
	for i, blocks := range curRackNodes {
		curID := rackID*config.RackSize + i
		if curID != rootD {
			//传输blocks到rootD
			for _, b := range blocks{
				common.SendCMD(common.GetNodeIP(curID), []string{common.GetNodeIP(rootD)}, b, b)
			}
		}
	}
	/****分发*****/
	for _, blocks := range parities {
		for _, b := range blocks {
			common.SendCMD(common.GetNodeIP(rootD), []string{common.GetNodeIP(rootD)}, b, b)
		}

	}
	sort.Ints(unionParities)
	fmt.Printf("ParityUpdate: stripe: %v, parities: %v, unionParities: %v, curRackNodes: %v\n",
											stripe, parities, unionParities, curRackNodes)
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
	//fmt.Printf("当前剩余ack：%d\n", RequireACKs)
	//popACK(ack.SID)
	ackMaps.popACK(ack.SID)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}
	}
}
func (p CAU) Clear()  {
	curDistinctBlocks = make([]int, 0, 100)
	sid = 0
	AckReceiverIPs = make(map[int]string)
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
}

func (p CAU) RecordSIDAndReceiverIP(sid int, ip string)()  {

}
func GetRootParityID(parities [][]int) int {
	for i, parity := range parities {
		if len(parity) > 0{
			return common.GetParityIDFromIndex(i)
		}
	}
	return -1
}
