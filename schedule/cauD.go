package schedule

import (
	"EC/common"
	"EC/config"
	"github.com/wxnacy/wgo/arrays"
	"log"
)
/*TUpdate:  delta + XOR + cau path + batch */
type CAU_D struct {
	Base
}
var curMatchReqs = make([]*config.ReqData, 0, config.MaxBatchSize)
func (p CAU_D) Init()  {
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
	actualBlocks = 0
	round = 0
	curReceivedTDs = &ReceivedTDs{
		TDs: make([]*config.TD, 0, config.MaxBatchSize),
	}
	sid = 0
	curMatchReqs = make([]*config.ReqData, 0, config.MaxBatchSize)
	ClearChannels()
}

func (p CAU_D) HandleTD(td *config.TD) {

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

	//有等待任务
	indexes := meetCMDNeed(td.BlockID)
	if len(indexes) > 0 {
		//添加ack监听
		for _, cmd := range indexes {
			log.Printf("执行TD任务：sid:%d blockID:%d\n", cmd.SID, cmd.BlockID)
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		//执行cmd
		for _, cmd := range indexes {
			xorBuff := getXORBuffFromCMD(cmd)
			toIP := cmd.ToIPs[0]
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff:    xorBuff,
				FromIP:  cmd.FromIP,
				ToIP:    toIP,
				SID:     cmd.SID,
				SendSize: cmd.SendSize,
			}
			common.SendData(td, toIP, config.NodeTDListenPort)
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
	//xorBuff := make([]byte, config.RSBlockSize)
	//var xorBuff [64 * config.Megabyte]byte
	//parityNodeID := common.GetIDFromIP(toIP)
	//row := parityNodeID - config.K
	//
	//log.Printf("parityNodeID=%v, row=%v, toIP=%v\n", parityNodeID, row, toIP)
	//
	for _, td := range mapBlockTDs {
		//col := b % config.K
		//log.Printf("col=%v, buffLen=%v, \n", col, len(td.Buff))
		//for i := 0; i < len(xorBuff); i++ {
		//	xorBuff[i]^= td.Buff[i]*config.RS.GenMatrix[row*config.K+col]
		//}
		return td.Buff
	}
	return  nil

	//return xorBuff
}
func getXORBuffFromCMD(cmd *config.CMD) []byte {
	mapBlockTDs := getMapBlockTDsFromHelpers(cmd.Helpers)
	//log.Printf("mapBlockTDs:= %v\n", mapBlockTDs)
	buff := getXORBuffFromMapBlockTDs(mapBlockTDs, cmd.ToIPs[0])

	return buff
}

func findRSDistinctBlocks() {
	curMatchReqs := make([]*config.ReqData, 0, config.MaxRSBatchSize)
	if len(totalReqs) > config.MaxRSBatchSize {
		curMatchReqs = totalReqs[:config.MaxRSBatchSize]
		for _, req := range curMatchReqs {
			if arrays.Contains(curDistinctBlocks, req.BlockID) < 0 {
				curDistinctBlocks = append(curDistinctBlocks, req.BlockID)
			}
		}
		totalReqs = totalReqs[config.MaxRSBatchSize:]
	}else { //处理最后不到100个请求
		curMatchReqs = totalReqs
		for _, req := range curMatchReqs {
			if arrays.Contains(curDistinctBlocks, req.BlockID) < 0 {
				curDistinctBlocks = append(curDistinctBlocks, req.BlockID)
			}
		}
		totalReqs = make([]*config.ReqData, 0, config.MaxBlockSize)
	}
}

func (p CAU_D) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		curMatchReqs = findDistinctBlocks()
		actualBlocks += len(curMatchReqs)
		log.Printf("第%d轮 CAU_D：处理%d个block\n", round, len(curDistinctBlocks))

		cau_d()

		for IsRunning {
			
		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}
}

func cau_d() {
	stripes := turnReqsToStripes(curMatchReqs)
	for _, stripe := range stripes{

		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {
				if compareRacks(i, ParityRackIndex, stripe) {
					dxr_parityUpdate(i, stripe)
				}else{
					dxr_dataUpdate(i, stripe)
				}
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
		//log.Printf("blockID: %d, nodeID: %d, rackID: %d\n", blockID, nodeID, rackID)
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
			log.Printf("pushACK: sid: %d, blockIDs: %v\n", curSid, parities[i])
			ackMaps.pushACK(curSid)
			curSid++
		}
	}

	/****分发*****/
	log.Printf("DataUpdate: parityNodeBlocks: %v\n", parities)

	for i := 1; i < len(parities); i++ {

		parityID := rootP + i
		for _, b := range parities[i]{
			_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(b, curMatchReqs)
			log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
				rootP, common.GetNodeIP(rootP), b, common.GetNodeIP(parityID))
			common.SendCMDWithSizeAndHelper(common.GetNodeIP(rootP), []string{common.GetNodeIP(parityID)},
												sid, b, rangeRight - rangeLeft, parities[i])

			sid++
			break
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
			_, rangeLeft, rangeRight := getBlockRangeFromDistinctReqs(b, curMatchReqs)

			log.Printf("sid : %d, 发送命令给 Node %d (%s)，使其将Block %d 发送给 %v\n", sid,
				nodeID, common.GetNodeIP(nodeID), b, common.GetNodeIP(rootP))

			//cmd := config.CMDBufferPool.Get().(*config.CMD)
			//cmd.SID = sid
			//cmd.BlockID = b
			//cmd.ToIPs = []string{common.GetNodeIP(rootP)}
			//cmd.FromIP = common.GetNodeIP(nodeID)
			//cmd.Helpers = make([]int, 0, 1)
			//cmd.Matched = 0
			//cmd.SendSize = rangeRight - rangeLeft
			//
			//common.SendData(cmd, common.GetNodeIP(nodeID), config.NodeCMDListenPort)

			//config.CMDBufferPool.Put(cmd)
			helpers := make([]int, 0, 1)
			common.SendCMDWithSizeAndHelper(common.GetNodeIP(nodeID), []string{common.GetNodeIP(rootP)},
				sid, b, rangeRight - rangeLeft, helpers)

			sid++
			//统计跨域流量
			totalCrossRackTraffic += rangeRight - rangeLeft
		}
	}
	log.Printf("DataUpdate: stripe: %v, parities: %v, curRackNodes: %v\n",
		stripe, parities, curRackNodes)
}

func (p CAU_D) HandleCMD(cmd *config.CMD)  {
	//helpers已到位
	if len(cmd.Helpers) == 0 {

		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}

		//读取本地数据
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)

		//执行命令
		for _, toIP := range cmd.ToIPs {
			td := config.TDBufferPool.Get().(*config.TD)
			td.BlockID = cmd.BlockID
			td.Buff = buff
			td.FromIP = cmd.FromIP
			td.ToIP = toIP
			td.SID = cmd.SID
			common.SendData(td, toIP, config.NodeTDListenPort)
			config.TDBufferPool.Put(td)
		}

		//内存回收
		config.BlockBufferPool.Put(buff)

	//helpers未到位
	}else{
		CMDList.pushCMD(cmd)
	}

}

func (p CAU_D) HandleACK(ack *config.ACK)  {
	restACKs := ackMaps.popACK(ack.SID)
	if restACKs == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		//检查是否全部完成，若完成，进入下一轮
		}else if ACKIsEmpty() {
			IsRunning = false
		}
	}
}
func (p CAU_D) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxRSBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxRSBatchSize)
	sid = 0
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxRSBatchSize),
	}
	curReceivedTDs = &ReceivedTDs{
		TDs: make([]*config.TD, 0, config.MaxRSBatchSize),
	}
	curMatchReqs = make([]*config.ReqData, 0, config.MaxBatchSize)
}

func (p CAU_D) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p CAU_D) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}

func (p CAU_D) GetActualBlocks() int {
	return actualBlocks
}

func (p CAU_D) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.Megabyte
}