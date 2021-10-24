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
	BlockID int
}

func TaskAdjust(taskGroup []Task)  {
	for _, t := range taskGroup {
		s, e :=  t.Start, t.End
		if s > e {
			t.Start, t.End = t.End, t.Start
		}
	}
}

type TUpdate struct {
	ReceivedTDs []config.TD
	CMDWaitingQueue []config.CMD
}

func (p TUpdate) Init()  {
	InitNetworkDistance()
	p.ReceivedTDs = make([]config.TD, 0, 100)
	p.CMDWaitingQueue = make([]config.CMD, 0, 100)
}

func (p TUpdate) HandleReq(reqs []config.ReqData)  {
	//sid := reqData.SID
	//blockID := reqData.BlockID
	//stripeID := reqData.StripeID
	//tasks := GetTransmitTasks(blockID)
	//for _, task := range tasks{
	//	fromIP := common.GetDataNodeIP(int(task.Start))
	//	toIP := common.GetDataNodeIP(int(task.End))
	//	toIPs := make([]string, 1)
	//	toIPs = append(toIPs, toIP)
	//	cmd := config.CMD{
	//		CreatorIP: common.GetLocalIP(),
	//		SID:      sid,
	//		StripeID: stripeID,
	//		BlockID:  blockID,
	//		ToIPs:    toIPs,
	//		FromIP:   fromIP,
	//	}
	//	fmt.Printf("发送命令给 node: %s，使其将Block %d 发送给 %v\n", fromIP, blockID, toIP)
	//	common.SendData(cmd, toIP, config.NodeCMDListenPort, "")
	//	PushWaitingACKGroup(cmd.SID, cmd.BlockID,1, common.GetLocalIP(), "")
	//}
}

func (p TUpdate) HandleTD(td config.TD)  {
	if p.meetCMDNeed(td) {
		cmd := p.getMeetCMD(td)
		p.finishCMD(cmd, td.Buff)
		p.deleteCMD(cmd)
	}
	//local update
	go common.WriteDeltaBlock(td.BlockID, td.Buff)
	//return ack
	ack := &config.ACK{
		SID:     td.SID,
		BlockID: td.BlockID,
	}
	common.SendData(ack, td.FromIP, config.NodeACKListenPort, "ack")
}

const MAX_COUNT int = 9
const INFINITY byte = 255
var nodeMatrix = make(config.Matrix, (config.K+config.M)*(config.K+config.M))
func InitNetworkDistance()  {
	//缺一个网络距离矩阵
	for i := 0; i < config.K+config.M; i++ {
		for j := 0; j < config.K+config.M; j++ {
			if i == j {
				nodeMatrix[i*(config.K+config.M)+j] = 0
			}else{
				nodeMatrix[i*(config.K+config.M)+j] = 5
			}
		}
	}
	nodeMatrix[0*(config.K+config.M)+1] = 1
	nodeMatrix[1*(config.K+config.M)+0] = 1
	nodeMatrix[2*(config.K+config.M)+3] = 1
	nodeMatrix[3*(config.K+config.M)+2] = 1
	nodeMatrix[4*(config.K+config.M)+5] = 1
	nodeMatrix[5*(config.K+config.M)+4] = 1
	nodeMatrix[6*(config.K+config.M)+7] = 1
	nodeMatrix[7*(config.K+config.M)+6] = 1
	nodeMatrix[8*(config.K+config.M)+9] = 1
	nodeMatrix[9*(config.K+config.M)+8] = 1
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
		fmt.Printf("(%d, %d) ", vertex[k], k)

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
func GetTransmitTasks(blockID int) []Task {
	parities :=	common.RelatedParities(blockID)
	nodeID := common.GetNodeID(blockID)
	relatedParityMatrix, nodeIndexs := getAdjacentMatrix(parities, nodeID, nodeMatrix)
	path := GetMSTPath(relatedParityMatrix, nodeIndexs)
	taskGroup := make([]Task, 0, len(nodeIndexs)-1)
	for i := 1; i < len(nodeIndexs); i++ {
		taskGroup = append(taskGroup, Task{Start: nodeIndexs[path[i]], BlockID: blockID, End:nodeIndexs[i]})
	}
	TaskAdjust(taskGroup)
	sort.SliceStable(taskGroup, func(i, j int) bool {
		return taskGroup[i].Start < taskGroup[j].Start
	})

	fmt.Printf("GetTransmitTasks :%v\n", taskGroup)

	return taskGroup

}
func getAdjacentMatrix(parities []byte, nodeID int, allMatrix []byte) (config.Matrix, config.Matrix) {
	nodeIndexes := make(config.Matrix, 0, (1+config.M)*(1+config.M))
	nodeIndexes = append(nodeIndexes, (byte)(nodeID/config.W))
	for i := 0; i < len(parities); i++ {
		if arrays.Contains(nodeIndexes, parities[i]/(byte)(config.W)) < 0 {
			nodeIndexes = append(nodeIndexes, parities[i]/(byte)(config.W))
		}
	}
	len := len(nodeIndexes) //[0 4 5]
	newMatrix := make(config.Matrix, len*len)
	for i := 0; i < len; i++ {
		for j := 0; j < len; j++ {
			value :=  nodeIndexes[i]*(byte)(config.K+config.M)+ nodeIndexes[j]
			newMatrix[i*len+j] = allMatrix[value]
		}
	}
	return newMatrix, nodeIndexes
}
func (p TUpdate) HandleCMD(cmd config.CMD)  {
	if p.IsCMDDataExist(cmd) {
		fmt.Printf("block %d is local\n", cmd.BlockID)
		buff := common.ReadBlock(cmd.BlockID)
		p.finishCMD(cmd, buff)
	}else{
		p.CMDWaitingQueue = append(p.CMDWaitingQueue, cmd)
	}
}
func (p TUpdate) meetCMDNeedAndReturnIndex(td config.TD) int {
	for i, cmd:= range p.CMDWaitingQueue{
		if cmd.SID == td.SID{
			return i
		}
	}
	return -1
}
func (p TUpdate) meetCMDNeed(td config.TD) bool  {
	for _, cmd:= range p.CMDWaitingQueue{
		if cmd.SID == td.SID{
			return true
		}
	}
	return false
}
func (p TUpdate) IsCMDDataExist(cmd config.CMD) bool {
	return common.GetDataNodeIP(cmd.BlockID) == common.GetLocalIP()
}
func (p TUpdate) finishCMD(cmd config.CMD, buff []byte) {
	for _, toIP := range cmd.ToIPs {
		common.SendData(buff, cmd.FromIP, toIP, "")
	}
	PushWaitingACKGroup(cmd.SID, cmd.BlockID, 1,  cmd.FromIP, "")
}
func (p TUpdate) getMeetCMD(td config.TD) config.CMD {
	for _, cmd:= range p.CMDWaitingQueue{
		if cmd.SID == td.SID{
			return cmd
		}
	}
	return config.CMD{}
}
func (p TUpdate) deleteCMD(delCMD config.CMD) {
	for i, cmd:= range p.CMDWaitingQueue{
		if cmd.SID == delCMD.SID{
			p.CMDWaitingQueue = append(p.CMDWaitingQueue[:i], p.CMDWaitingQueue[i:]...)
		}
	}
}
func (p TUpdate) HandleACK(ack config.ACK)  {
	PopWaitingACKGroup(ack.SID)
	if !IsExistInWaitingACKGroup(ack.SID) {
		ack := &config.ACK{
			SID:     ack.SID,
			BlockID: ack.BlockID,
		}
		ackReceiverIP := WaitingACKGroup[ack.SID].ACKReceiverIP
		common.SendData(ack, ackReceiverIP, config.NodeACKListenPort, "ack")

		delete(WaitingACKGroup, ack.SID)
	}
}
func (p TUpdate) Clear()  {
	p.ReceivedTDs = make([]config.TD, 0, 100)
	p.CMDWaitingQueue = make([]config.CMD, 0, 100)
}

func (p TUpdate) RecordSIDAndReceiverIP(sid int, ip string)()  {

}