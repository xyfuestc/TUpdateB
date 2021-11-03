package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
	"sort"
)
type Graph struct {
	N   int //顶点数
	M   int //边数
	Arc [][]byte
}
type Task struct {
	Start byte
	End   byte
	SID   int
	BlockID int
}

type TUpdate struct {
	CMDWaitingQueue []*config.CMD
}
const MAX_COUNT int = 9
const INFINITY byte = 255
var NodeMatrix = make(config.Matrix, (config.N)*(config.N))
func TaskAdjust(taskGroup []Task)  {
	for _, t := range taskGroup {
		s, e :=  t.Start, t.End
		if s > e {
			t.Start, t.End = t.End, t.Start
		}
	}
}
func (p TUpdate) Init()  {
	InitNetworkDistance()
	ackMaps = &ACKMap{
		RequireACKs: make(map[int]int),
	}
	ackIPMaps = &ACKIPMap{
		ACKReceiverIPs: map[int]string{},
	}
	p.CMDWaitingQueue = make([]*config.CMD, 0, config.MaxBatchSize)
}

func (p TUpdate) HandleReq(blocks []int)  {
	for _, _ = range blocks {
		ackMaps.pushACK(sid)
		sid++
	}

	sid = 0
	for _, b := range blocks {
		req := &config.ReqData{
			BlockID: b,
			SID: sid,
		}
		p.handleOneBlock(req)
		sid++
	}
}

func (p TUpdate) handleOneBlock(reqData * config.ReqData)  {
	tasks := GetTransmitTasks(reqData)
	fmt.Printf("tasks: %v\n", tasks)
	for _, task := range tasks {
		fromIP := common.GetNodeIP(int(task.Start))
		toIPs := []string{common.GetNodeIP(int(task.End))}
		common.SendCMD(fromIP, toIPs, task.SID, task.BlockID)
	}
}

func (p TUpdate) HandleTD(td *config.TD)  {
	//本地数据更新
	go common.WriteDeltaBlock(td.BlockID, td.Buff)
	//有等待任务
	indexes := p.meetCMDNeed(td)
	if len(indexes) > 0 {
		//添加ack监听
		for _, i := range indexes {
			cmd := p.CMDWaitingQueue[i]
			fmt.Printf("cmd : %v\n", cmd)
			for _, _ = range cmd.ToIPs {
				ackMaps.pushACK(cmd.SID)
			}
		}
		for _, i := range indexes {
			cmd := p.CMDWaitingQueue[i]
			for _, toIP := range cmd.ToIPs {
				common.SendData(td.Buff, toIP, config.NodeTDListenPort, "")
			}
			p.CMDWaitingQueue = append(p.CMDWaitingQueue[:i], p.CMDWaitingQueue[i:]...)
		}
	}else{
		fmt.Printf("CMDWaitingQueue: %v\n", p.CMDWaitingQueue)
		//没有等待任务，返回ack
		if n, ok := ackMaps.getACK(td.SID); !ok || n == 0 {
			//返回ack
			ack := &config.ACK{
				SID:     td.SID,
				BlockID: td.BlockID,
			}
			ReturnACK(ack)
		}
	}
}

func InitNetworkDistance()  {
	for i := 0; i < config.N; i++ {
		for j := 0; j < config.N; j++ {
			if i == j {
				NodeMatrix[i*config.N+j] = 0
			}else{
				NodeMatrix[i*config.N+j] = 2
			}
		}
	}
	//初始化Rack内部网络距离
	for r := 0; r < config.N / config.RackSize; r++ {
		curRackMinNode := config.RackSize * r
		curRackMaxNode := config.RackSize * r + config.RackSize
		for i := curRackMinNode; i < curRackMaxNode; i++ {
			for j := curRackMinNode; j < curRackMaxNode; j++ {
				if i != j {
					NodeMatrix[i*config.N+j] = 1
				}
			}
		}
	}
}
/*Prim算法*/
func Prim(G Graph) config.Matrix{
	var lowCost = [MAX_COUNT]byte{}
	var vertex = make(config.Matrix, MAX_COUNT)
	lowCost[0] = 0
	for j := 1; j < G.N; j++ {
		lowCost[j] = G.Arc[0][j]
		vertex[j] = 0
	}
	for i := 1; i < G.N; i++ {
		k := 1
		min := INFINITY
		for j := 1; j < G.N; j++ {
			if lowCost[j] != 0 && lowCost[j] < min {
				min = lowCost[j]
				k = j
			}
		}
		lowCost[k] = 0
		for j := 0; j < G.N; j++ {
			if lowCost[j] != 0 && G.Arc[k][j] < lowCost[j] {
				lowCost[j] = G.Arc[k][j]
				vertex[j] = byte(k)
			}
		}
	}
	return vertex
}
/*构造函数*/
func NewGraph(N int) Graph {
	buf := make([][]byte, N)
	for i := 0; i < N; i++ {
		buf[i] = make([]byte, N)
	}
	return Graph{
		N: N,
		M: 0,
		Arc: buf,
	}
}
/*测试*/
func GetMSTPath(matrix, nodeIndexs config.Matrix) config.Matrix   {
	len := len(nodeIndexs)
	G := NewGraph(len)
	for i := 0; i < G.N; i++ {
		for j := 0; j < G.N; j++ {
			G.Arc[i][j] = matrix[i*len+j]
		}
	}
	path := Prim(G)
	return path
}
func GetTransmitTasks(reqData *config.ReqData) []Task {
	parities :=	common.RelatedParities(reqData.BlockID)
	parityNodes := common.RelatedParityNodes(parities)
	nodeID := common.GetNodeID(reqData.BlockID)
	relatedParityMatrix, nodeIndexs := getAdjacentMatrix(parityNodes, nodeID, NodeMatrix)
	path := GetMSTPath(relatedParityMatrix, nodeIndexs)
	taskGroup := make([]Task, 0, len(nodeIndexs)-1)
	for i := 1; i < len(nodeIndexs); i++ {
		taskGroup = append(taskGroup, Task{Start: nodeIndexs[path[i]], SID: reqData.SID, BlockID: reqData.BlockID, End:nodeIndexs[i]})
	}
	TaskAdjust(taskGroup)
	sort.SliceStable(taskGroup, func(i, j int) bool {
		return taskGroup[i].Start < taskGroup[j].Start
	})

	//fmt.Printf("GetTransmitTasks :%v\n", taskGroup)

	return taskGroup

}
func getAdjacentMatrix(parities []byte, nodeID int, allMatrix []byte) (config.Matrix, config.Matrix) {
	nodeIDs := make(config.Matrix, 0, (1+config.M)*(1+config.M))
	nodeIDs = append(nodeIDs, (byte)(nodeID))
	for i := 0; i < len(parities); i++ {
		if arrays.Contains(nodeIDs, parities[i]) < 0 {
			nodeIDs = append(nodeIDs, parities[i])
		}
	}
	len := len(nodeIDs) //[0 4 5]
	newMatrix := make(config.Matrix, len*len)
	for i := 0; i < len; i++ {
		for j := 0; j < len; j++ {
			value :=  nodeIDs[i]*(byte)(config.N)+ nodeIDs[j]
			newMatrix[i*len+j] = allMatrix[value]
		}
	}
	return newMatrix, nodeIDs
}
func (p TUpdate) HandleCMD(cmd *config.CMD)  {
	if IsCMDDataExist(cmd) {
		//添加ack监听
		for _, _ = range cmd.ToIPs {
			ackMaps.pushACK(cmd.SID)
		}
		fmt.Printf("block %d is local\n", cmd.BlockID)
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
		p.CMDWaitingQueue = append(p.CMDWaitingQueue, cmd)
		fmt.Printf("需要处理的cmd ：%v\n", p.CMDWaitingQueue)
	}
}
func (p TUpdate) meetCMDNeed(td *config.TD) []int  {
	indexes := make([]int, 0, config.M)
	for i, cmd := range p.CMDWaitingQueue{
		if cmd.SID == td.SID{
			indexes = append(indexes, i)
		}
	}
	return indexes
}
func IsCMDDataExist(cmd *config.CMD) bool {
	return common.GetNodeIP(common.GetNodeID(cmd.BlockID)) == common.GetLocalIP()
}

func (p TUpdate) getMeetCMD(td *config.TD) *config.CMD {
	for _, cmd:= range p.CMDWaitingQueue{
		if cmd.SID == td.SID{
			return cmd
		}
	}
	return &config.CMD{}
}

func (p TUpdate) HandleACK(ack *config.ACK)  {
	ackMaps.popACK(ack.SID)
	if v, _ := ackMaps.getACK(ack.SID) ; v == 0 {
		//ms不需要反馈ack
		if common.GetLocalIP() != config.MSIP {
			ReturnACK(ack)
		}
	}
}
func (p TUpdate) Clear()  {
	p.CMDWaitingQueue = make([]*config.CMD, 0, 100)
	NodeMatrix = make(config.Matrix, (config.N)*(config.N))
}

func (p TUpdate) RecordSIDAndReceiverIP(sid int, ip string)()  {
	ackIPMaps.recordIP(sid, ip)
}

func (p TUpdate) IsFinished() bool {
	return ackMaps.isEmpty()
}

