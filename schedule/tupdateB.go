package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"log"
	"math"
	"sort"
)
/*TUpdate:  delta + handle one block + XOR + tree-structured path + batch */
var Space = 16385  //for 0.25MB
//var Space = 10000  //for 1MB
//var Space = 10000 // for 4MB
var Done = make(chan bool, 1)
type TUpdateB struct {

}

type MergeBlockCandidate struct {
	BlockID int
	averageSpace float64
	Reqs    []*config.ReqData
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
		mergeReqs,_ := BlockMergeWithSpace(curMatchReqs, Space)
		actualBlocks += len(mergeReqs)

		log.Printf("第%d轮 TUpdateB：获取%d个请求，实际处理%d个block\n", round, len(curMatchReqs), len(mergeReqs))

		//执行reqs
		p.TUpdateB(mergeReqs)

		select {
		case <-Done:
			log.Printf("本轮结束！\n")
			log.Printf("======================================\n")
			round++
			p.Clear()
		}

	}
}

func (p TUpdateB) TUpdateB(reqs []*config.ReqData)   {

	oldSid := sid
	//记录ack
	for _, _ = range reqs {
		ackMaps.pushACK(sid)
		sid++
	}
	sid = oldSid
	//处理reqs
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
		sendSizeKB := float32(SendSize) / 1024.0 //转为KB
		log.Printf("task %d: send block %d with size %fKB from %s to %v.\n", task.SID, task.BlockID, sendSizeKB, fromIP, toIPs)

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
			Done <- true
		}
	}
}
func (p TUpdateB) Clear()  {
	IsRunning = true
	curDistinctBlocks = make([]int, 0, config.MaxBatchSize)
	curDistinctReq = make([]*config.ReqData, 0, config.MaxBatchSize)
	//sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}
func (p TUpdateB) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdateB) IsFinished() bool {
	return len(totalReqs) == 0 && ACKIsEmpty()
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
		if sum <= 0 && len(blockMap) > 1 {
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

func BlockMergeWithSpace(reqs []*config.ReqData, space int) ([]*config.ReqData, int) {

	nextSpace := math.MaxInt32
	mergeReqs := make([]*config.ReqData, 0, len(reqs))
	blockMaps := make(map[int][]*config.ReqData, 1000)
	averageSpaceIncrement := 0

	for _,req := range reqs {
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
		//至少有2个blocks
		averageSpaceIncrement = sum / len(blockMap) + 1
		if averageSpaceIncrement < space && len(blockMap) > 1 {
			//fmt.Println( blockID, " 可以合并: ", sum)

			newMergeBlock := &config.ReqData{
				BlockID: blockID,
				RangeLeft: blockMap[0].RangeLeft,
				RangeRight: blockMap[len(blockMap)-1].RangeRight,
			}
			mergeReqs = append(mergeReqs, newMergeBlock)
		//sum > space
		}else {
			//fmt.Println( blockID, " 建议不合并: ", sum)
			mergeReqs = append(mergeReqs, blockMap...)
			//取下一次最小的步长
			if nextSpace > averageSpaceIncrement && space < averageSpaceIncrement {
				nextSpace = averageSpaceIncrement
			}
		}
	}

	//for _, req := range mergeReqs {
	//	fmt.Printf( "%d [%d, %d]\n", req.BlockID, req.RangeLeft, req.RangeRight)
	//}
	//完全批处理
	if nextSpace == math.MaxInt32 {
		nextSpace = -1
	}

	return mergeReqs, nextSpace
}

func BlockMergeWithAverageSpace(reqs []*config.ReqData, space float64) ([]*config.ReqData, float64) {

	nextSpace := space
	mergeReqs := make([]*config.ReqData, 0, len(reqs))
	blockMaps := make(map[int][]*config.ReqData, 1000)
	averageSpaceIncrement := 0.0
	//singleBlocks := make([]*config.ReqData, 0, len(reqs))
	Blocks := make([]*MergeBlockCandidate, 0, len(reqs))

	//按照blockID分组，放到blockMaps[blockID]
	for _,req := range reqs {
		blockMaps[req.BlockID] = append(blockMaps[req.BlockID], req)
	}


	for blockID, blockMap := range blockMaps {

		//发现该组只有一个块，不需要合并，直接放入mergeReqs
		if len(blockMap) == 1 {
			mergeReqs = append(mergeReqs, blockMap[0])
			continue
		}

		//将blockMap按照rangeL从小到大排序
		sort.SliceStable(blockMap, func(i, j int) bool {
			return blockMap[i].RangeLeft < blockMap[j].RangeLeft
		})
		//求和
		sum := 0
		for i := 0; i < len(blockMap) - 1; i++ {
			sum += blockMap[i+1].RangeLeft - blockMap[i].RangeRight
		}
		//求平均增长值
		averageSpaceIncrement = float64(sum) / float64(len(blockMap))
		//将blockID-averageSpaceIncrement-blockMap放入临时变量Blocks数组
		Blocks = append(Blocks, &MergeBlockCandidate{BlockID: blockID, averageSpace: averageSpaceIncrement, Reqs: blockMap})

	}
	//按照rangeL从小到大排序
	sort.SliceStable(Blocks, func(i, j int) bool {
		return Blocks[i].averageSpace < Blocks[j].averageSpace
	})

	//根据给定space找出应该合并的最大索引i
	curAverageSpace := 0.0
	i := 0
	for ; i < len(Blocks); i++ {
		curAverageSpace += Blocks[i].averageSpace
		if curAverageSpace / float64(i+1) > space {
			nextSpace = curAverageSpace / float64(i+1)
			break
		}
	}
	for j := 0; j < i-1; j++ {
		newMergeBlock := &config.ReqData{
			BlockID: Blocks[j].BlockID,
			RangeLeft: Blocks[j].Reqs[0].RangeLeft,
			RangeRight: Blocks[j].Reqs[len(Blocks[j].Reqs)-1].RangeRight,
		}
		mergeReqs = append(mergeReqs, newMergeBlock)
	}
	//单独处理，不合并
	for k := i-1; k < len(Blocks); k++ {
		mergeReqs = append(mergeReqs, Blocks[k].Reqs...)

	}
	if nextSpace == space {
		 nextSpace = -1
	}

	return mergeReqs, nextSpace
}
