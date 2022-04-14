package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"log"
	"sort"
)
/*TUpdate:  delta + handle one block + XOR + tree-structured path + batch */

type TUpdateB struct {

}

func (p TUpdateB) Init()  {

	InitNetworkDistance()
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}

	totalCrossRackTraffic = 0
	actualBlocks = 0
	round = 0
	sid = 0

	ClearChannels()
}

func (p TUpdateB) HandleReq(reqs []*config.ReqData)  {

	totalReqs = reqs
	log.Printf("一共接收到%d个请求...\n", len(totalReqs))

	for len(totalReqs) > 0 {
		//过滤blocks
		curMatchReqs := FindDistinctBlocks()
		mergeReqs := BlockMerge(curMatchReqs)
		actualBlocks += len(mergeReqs)

		log.Printf("第%d轮 TUpdateB：获取%d个请求，实际处理%d个block\n", round, len(curMatchReqs), len(mergeReqs))

		//执行reqs
		p.TUpdateB(mergeReqs)

		for IsRunning {

		}
		log.Printf("本轮结束！\n")
		log.Printf("======================================\n")
		round++
		p.Clear()
	}
}

func (p TUpdateB) TUpdateB(reqs []*config.ReqData)   {

	//记录ack
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}

	//处理reqs
	sid = 0
	for _, req := range reqs {
		req.SID = sid
		p.handleOneReq(req)
		sid++
	}
}

func (p TUpdateB) handleOneReq(reqData * config.ReqData)  {
	tasks := GetBalanceTransmitTasks(reqData)
	//tasks := GetTransmitTasks(reqData)
	log.Printf("tasks: %v\n", tasks)
	for _, task := range tasks {

		//构造cmd
		fromIP := common.GetNodeIP(int(task.Start))
		toIPs := []string{common.GetNodeIP(int(task.End))}
		SendSize := reqData.RangeRight - reqData.RangeLeft
		helpers := make([]int, 0, 1)

		common.SendCMDWithSizeAndHelper(fromIP, toIPs, task.SID, task.BlockID, SendSize, helpers)

		//统计跨域流量
		rack1 := getRackIDFromNodeID(task.Start)
		rack2 := getRackIDFromNodeID(task.End)
		if rack1 != rack2 {
			totalCrossRackTraffic += SendSize
		}
	}
}

func (p TUpdateB) HandleTD(td *config.TD)  {

	//本地数据更新
	common.WriteDeltaBlock(td.BlockID, td.Buff)

	//有可以执行的等待任务
	cmds := CMDList.popRunnableCMDsWithSID(td.SID)
	if len(cmds) > 0 {
		//添加ack监听
		for _, cmd := range cmds {
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		for _, cmd := range cmds {
			for _, toIP := range cmd.ToIPs {
				SendTD := &config.TD{
					BlockID: cmd.BlockID,
					Buff: td.Buff,
					FromIP: cmd.FromIP,
					ToIP: toIP,
					SID: cmd.SID,
					SendSize: cmd.SendSize,
				}
				//sendSizeRate := float32(SendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
				//log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", SendTD.BlockID, sendSizeRate, toIP)
				//common.SendData(SendTD, toIP, config.NodeTDListenPort, "")

				//SendTD := config.TDBufferPool.Get().(*config.TD)
				//SendTD.BlockID = cmd.BlockID
				//SendTD.Buff = td.Buff[:cmd.SendSize]
				//SendTD.FromIP = cmd.FromIP
				//SendTD.ToIP = toIP
				//SendTD.SID = cmd.SID
				//SendTD.SendSize = cmd.SendSize
				sendSizeRate := float32(SendTD.SendSize * 1.0) / float32(config.BlockSize) * 100.0
				log.Printf("发送 block:%d sendSize: %.2f%% -> %s.\n", SendTD.BlockID, sendSizeRate, toIP)
				common.SendData(SendTD, toIP, config.NodeTDListenPort)

				config.TDBufferPool.Put(SendTD)
			}
		}
	}else{
		if _, ok := ackMaps.getACK(td.SID); !ok {
			//返回ack
			ack := &config.ACK{
				SID:     td.SID,
				BlockID: td.BlockID,
			}
			ReturnACK(ack)
		}
	}
}

func (p TUpdateB) HandleCMD(cmd *config.CMD)  {

	//helpers已到位
	if IsCMDDataExist(cmd) {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		buff := common.ReadBlockWithSize(cmd.BlockID, cmd.SendSize)

		for _, toIP := range cmd.ToIPs {
			td := &config.TD{
				BlockID: cmd.BlockID,
				Buff: buff,
				FromIP: cmd.FromIP,
				ToIP: toIP,
				SID: cmd.SID,
			}
			//td := config.TDBufferPool.Get().(*config.TD)
			//td.BlockID = cmd.BlockID
			//td.Buff = buff
			//td.FromIP = cmd.FromIP
			//td.ToIP = toIP
			//td.SID = cmd.SID
			common.SendData(td, toIP, config.NodeTDListenPort)

			//config.TDBufferPool.Put(td)
		}
		config.BlockBufferPool.Put(buff)

		//helpers未到位
	}else{
		cmd.Helpers = append(cmd.Helpers, cmd.BlockID)
		log.Printf("添加sid: %d, blockID: %d, helpers: %v到cmdList.\n", cmd.SID, cmd.BlockID, cmd.Helpers)
		CMDList.pushCMD(cmd)
	}
}
func (p TUpdateB) HandleACK(ack *config.ACK)  {
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
func (p TUpdateB) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}
func (p TUpdateB) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdateB) IsFinished() bool {
	return len(totalReqs) == 0 && ackMaps.isEmpty()
}
func (p TUpdateB) GetActualBlocks() int {
	return actualBlocks
}

func (p TUpdateB) GetCrossRackTraffic() float32 {
	return  float32(totalCrossRackTraffic) / config.Megabyte
}

func BlockMerge(reqs []*config.ReqData) []*config.ReqData {

	mergeReqs := make([]*config.ReqData, 0, len(reqs))
	blockMaps := make(map[int][]*config.ReqData, 1000)

	for _,req := range reqs {
		//if _, ok := blockMaps[req.BlockID]; !ok  {
		//	blockMaps[req.BlockID] = make([]*config.ReqData, 0, 100)
		//}
		blockMaps[req.BlockID] = append(blockMaps[req.BlockID], req)
	}

	for blockID, blockMap := range blockMaps {
		//按照rangeL从小到大排序
		sort.SliceStable(blockMap, func(i, j int) bool {
			return blockMap[i].RangeLeft < blockMap[j].RangeLeft
		})
		sum := 0

		for i := 0; i < len(blockMap) - 1; i++ {
			sum += blockMap[i+1].RangeLeft - blockMap[i].RangeRight
		}
		if sum < 0 {
			fmt.Println( blockID, " 可以合并: ", sum)

			newMergeBlock := &config.ReqData{
				BlockID: blockID,
				RangeLeft: blockMap[0].RangeLeft,
				RangeRight: blockMap[len(blockMap)-1].RangeRight,
			}
			mergeReqs = append(mergeReqs, newMergeBlock)
		}else {
			fmt.Println( blockID, " 建议不合并: ", sum)

			mergeReqs = append(mergeReqs, blockMap...)
		}
	}

	for _, req := range mergeReqs {
		fmt.Printf( "%d [%d, %d]\n", req.BlockID, req.RangeLeft, req.RangeRight)
	}

	return mergeReqs
}

