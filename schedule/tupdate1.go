package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"sort"
)

type TUpdate1 struct {

}

func (p TUpdate1) Init()  {
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
	actualBlocks = 0
}

func (p TUpdate1) HandleReq(blocks []int)  {
	totalBlocks = blocks

	for len(totalBlocks) > 0 {
		//过滤blocks
		findDistinctBlocks()
		//执行cau
		actualBlocks += len(curDistinctBlocks)
		fmt.Printf("第%d轮 CAU：sid：[%d, %d], 处理%d个block\n", round, sid, sid+len(curDistinctBlocks)-1, len(curDistinctBlocks))

		curSid := sid
		//记录ack
		for _, _ = range curDistinctBlocks {
			ackMaps.pushACK(curSid)
			curSid++
		}
		//处理block
		for _, b := range curDistinctBlocks {
			req := &config.ReqData{
				BlockID: b,
				SID: sid,
			}
			p.handleOneBlock(req)
			sid++
		}
		round++
	}
}

func (p TUpdate1) handleOneBlock(reqData * config.ReqData)  {
	tasks := GetBalanceTransmitTasks(reqData)
	fmt.Printf("tasks: %v\n", tasks)
	for _, task := range tasks {
		fromIP := common.GetNodeIP(int(task.Start))
		toIPs := []string{common.GetNodeIP(int(task.End))}
		common.SendCMD(fromIP, toIPs, task.SID, task.BlockID)
	}
}

func (p TUpdate1) HandleTD(td *config.TD)  {
	//本地数据更新
	go common.WriteDeltaBlock(td.BlockID, td.Buff)

	//有等待任务
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
				td := &config.TD{
					BlockID: cmd.BlockID,
					Buff: td.Buff,
					FromIP: cmd.FromIP,
					ToIP: toIP,
					SID: cmd.SID,
				}
				common.SendData(td, toIP, config.NodeTDListenPort, "")
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
func  GetBalanceTransmitTasks(reqData *config.ReqData) []Task {
	parities :=	common.RelatedParities(reqData.BlockID)
	parityNodes := common.RelatedParityNodes(parities)
	nodeID := common.GetNodeID(reqData.BlockID)
	relatedParityMatrix, nodeIndexs := getAdjacentMatrix(parityNodes, nodeID, NodeMatrix)
	path := GetMSTPath(relatedParityMatrix, nodeIndexs)


	bPath := getBalancePath(path, nodeIndexs)
	fmt.Printf("bPath : %v\n", bPath)

	taskGroup := make([]Task, 0, len(nodeIndexs)-1)
	for i := 1; i < len(nodeIndexs); i++ {
		taskGroup = append(taskGroup, Task{Start: nodeIndexs[path[i]], SID: reqData.SID, BlockID: reqData.BlockID, End:nodeIndexs[i]})
	}
	TaskAdjust(taskGroup)
	sort.SliceStable(taskGroup, func(i, j int) bool {
		return taskGroup[i].Start < taskGroup[j].Start
	})
	return taskGroup
}
func (p TUpdate1) HandleCMD(cmd *config.CMD)  {
	if IsCMDDataExist(cmd) {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		//fmt.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlock(cmd.BlockID)

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
		cmd.Helpers = append(cmd.Helpers, cmd.BlockID)
		fmt.Printf("添加sid: %d, blockID: %d, helpers: %v到cmdList.", cmd.SID, cmd.BlockID, cmd.Helpers)
		CMDList.pushCMD(cmd)
	}
}
func (p TUpdate1) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}
	}
}
func (p TUpdate1) Clear()  {
	sid = 0
	CMDList = &CMDWaitingList{
		Queue: make([]*config.CMD, 0, config.MaxBatchSize),
	}
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
	actualBlocks = 0
}
func (p TUpdate1) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}
func (p TUpdate1) IsFinished() bool {
	return ackMaps.isEmpty()
}
func (p TUpdate1) GetActualBlocks() int {
	return actualBlocks
}