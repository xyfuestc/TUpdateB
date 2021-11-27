package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
)
type CAURS struct {
	Base
}


func (p CAURS) Init()  {
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
	actualBlocks = 0
	round = 0
}

func (p CAURS) HandleTD(td *config.TD) {

	curReceivedTDs.pushTD(td)
	
	//校验节点本地数据更新
	localID := arrays.Contains(config.NodeIPs, common.GetLocalIP())
	if localID >= config.K {
		common.WriteDeltaBlock(td.BlockID, td.Buff)
	}

	//返回ack
	ack := &config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	ReturnACK(ack)

	//有等待任务
	indexes := meetCMDNeed(td.BlockID)
	if len(indexes) > 0 {
		//fmt.Printf("有等待任务可以执行：%v\n", indexes)
		//添加ack监听
		for _, cmd := range indexes {
			fmt.Printf("执行TD任务：sid:%d blockID:%d\n", cmd.SID, cmd.BlockID)
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		for _, cmd := range indexes {
			xorBuff := getXORBuffFromCMD(cmd)
			toIP := cmd.ToIPs[0]
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff:    xorBuff,
				FromIP:  cmd.FromIP,
				ToIP:    toIP,
				SID:     cmd.SID,
			}
			common.SendData(td, toIP, config.NodeTDListenPort, "")
		}
	}
}
func getMapBlockTDsFromHelpers(helpers []int) map[int]*config.TD  {
	//传输数之前，应该有一个计算校验更新的过程
	mapBlockTDs := map[int]*config.TD{}
	for _, v := range curReceivedTDs.getTDs() {
		for _, b := range helpers{
			if b == v.BlockID {
				mapBlockTDs[b] = v
			}
		}
	}
	return mapBlockTDs
}
func getXORBuffFromMapBlockTDs(mapBlockTDs map[int]*config.TD, toIP string) []byte {
	xorBuff := make([]byte, config.RSBlockSize)
	parityNodeID := common.GetIDFromIP(toIP)
	row := parityNodeID - config.K

	for b, td := range mapBlockTDs {
		col := b % config.K
		for i := 0; i < len(xorBuff); i++ {
			xorBuff[i]^= td.Buff[i]*config.RS.GenMatrix[row*config.K+col]
		}
	}
	return xorBuff
}
func getXORBuffFromCMD(cmd *config.CMD) []byte {
	mapBlockTDs := getMapBlockTDsFromHelpers(cmd.Helpers)
	buff := getXORBuffFromMapBlockTDs(mapBlockTDs, cmd.ToIPs[0])

	return buff
}

func (p CAURS) HandleReq(reqs []*config.ReqData)  {
	totalReqs = reqs
	fmt.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		findDistinctBlocks()
		//执行cau
		actualBlocks += len(curDistinctBlocks)
		fmt.Printf("第%d轮 CAU：处理%d个block\n", round, len(curDistinctBlocks))

		cau_rs()

		for IsRunning {
			
		}
		fmt.Printf("本轮结束！\n")
		fmt.Printf("======================================\n")
		round++
		p.Clear()
	}
}

func cau_rs() {
	stripes := turnBlocksToStripes()
	for _, stripe := range stripes{
		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {
					dataUpdateRS(i, stripe)
			}
		}
	}
}

func dataUpdateRS(rackID int, stripe []int)  {
	curRackNodes := make([][]int, config.RackSize)
	parities := make([][]int, config.M)
	for _, blockID := range stripe{
		nodeID := blockID % config.K
		//过滤掉与rackID不相关的block
		if byte(rackID) != getRackIDFromNodeID(byte(nodeID)) {
			continue
		}
		//fmt.Printf("blockID: %d, nodeID: %d, rackID: %d\n", blockID, nodeID, rackID)
		curRackNodes[nodeID-rackID*config.RackSize] = append(curRackNodes[nodeID-rackID*config.RackSize], blockID)
		for p, _ := range parities {
			parities[p] = append(parities[p], blockID)
		}
	}

	//选择一个rootP
	rootP := config.K  //选择第一个parityNode

	/****记录ack*****/
	curSid := sid

	/****记录ack*****/
	for i := 1; i < len(parities); i++ {
		if  len(parities[i]) > 0 {
			fmt.Printf("pushACK: sid: %d, blockIDs: %v\n", curSid, parities[i])
			ackMaps.pushACK(curSid)
			curSid++
		}
	}

	/****分发*****/
	fmt.Printf("DataUpdate: parityNodeBlocks: %v\n", parities)

	for i := 1; i < len(parities); i++ {
		parityID := rootP + i
		for _, b := range parities[i]{
			fmt.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
				rootP, common.GetNodeIP(rootP), b, common.GetNodeIP(parityID))

			cmd := &config.CMD{
				SID: sid,
				BlockID: b,
				ToIPs: []string{common.GetNodeIP(parityID)},
				FromIP: common.GetNodeIP(rootP),
				Helpers: parities[i],
				Matched: 0,
				SendSize: config.RSBlockSize,
			}
			common.SendData(cmd, common.GetNodeIP(rootP), config.NodeCMDListenPort, "")
			sid++
			break
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
			cmd := &config.CMD{
				SID: sid,
				BlockID: b,
				ToIPs: []string{common.GetNodeIP(rootP)},
				FromIP: common.GetNodeIP(nodeID),
				Helpers: make([]int, 0, 1),
				Matched: 0,
				SendSize: config.RSBlockSize,
			}
			common.SendData(cmd, common.GetNodeIP(nodeID), config.NodeCMDListenPort, "")
			sid++
		}
	}
	fmt.Printf("DataUpdate: stripe: %v, parities: %v, curRackNodes: %v\n",
		stripe, parities, curRackNodes)
}


func (p CAURS) HandleCMD(cmd *config.CMD)  {
	//handleOneCMD(cmd)
	if len(cmd.Helpers) == 0 {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		//fmt.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)

		for _, toIP := range cmd.ToIPs {
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff: buff,
				FromIP: cmd.FromIP,
				ToIP: toIP,
				SID: cmd.SID,
			}
			common.SendData(td, toIP, config.NodeTDListenPort, "")
		}
	}else{
		CMDList.pushCMD(cmd)
	}
}

func (p CAURS) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
	//fmt.Printf("当前剩余ack：%d\n", ackMaps)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}else if ACKIsEmpty() { //ms检查是否全部完成，若完成，进入下一轮
			IsRunning = false
		}
	}
}
func (p CAURS) Clear()  {
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
	//round = 0
}

func (p CAURS) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p CAURS) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}


func (p CAURS) GetActualBlocks() int {
	return actualBlocks
}

