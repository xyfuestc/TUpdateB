package schedule

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
)
type CAU struct {
	Base
}
//var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var NumOfCurNeedUpdateBlocks = 0
var round = 0
var IsRunning = false   //check CAU is running or not
const maxBatchSize = 100
var totalReqChunks = make([]config.ReqData, config.MaxBatchSize, config.MaxBatchSize)

func (p CAU) Init()  {

}

func (p CAU) HandleTD(td config.TD)  {

	targetIP := td.FromIP
	switch td.OPType {
	case config.OP_BASE:
		common.WriteBlock(td.BlockID, td.Buff)
		//return ack
		ack := &config.ACK{
			BlockID: td.BlockID,
			SID: td.SID,
		}
		common.SendData(ack, targetIP, config.NodeACKListenPort, "ack")
	//DDU模式，rootP接收到数据
	case config.DDURoot:
		//role := config.Role_DDURootP
		delta := td.Buff
		oldBuff := common.ReadBlock(td.BlockID)
		//默认rootP为P0
		row := 0
		newBuff := make([]byte, config.ChunkSize)
		//找到对应的Di
		if config.ECMode == "RS" {
			col := td.BlockID % config.K
			factor := config.RS.GenMatrix[row*config.K+col]
			for i := 0; i < len(delta); i++ {
				newBuff[i] = oldBuff[i] ^ config.Gfmul(factor, delta[i])    //Pi`=Pi+Aij*delta
			}
		}else{  //XOR
			for i := 0; i < len(delta); i++ {
				newBuff[i] = oldBuff[i] ^ delta[i]   //Pi`=Pi+Aij*delta
			}
		}
		common.WriteBlock(td.BlockID, newBuff)
		fmt.Printf("CMD_DDU: rootP update success!\n")
		fmt.Printf("transfer data to other leafs...\n")
		//send update data to leafs, wait for ack
		for _, leafIP := range td.NextIPs {
			//send data to leaf
			data := &config.TD{
				OPType:  config.DDULeaf,
				Buff:    delta,
				BlockID: td.BlockID,
				ToIP:    leafIP,
			}
			common.SendData(data, leafIP, config.NodeTDListenPort, "")
		}
		ack := &config.ACK{
			BlockID: td.BlockID,
			SID: td.SID,
		}
		fmt.Printf("return the ack of chunk %d to client...\n", td.BlockID)
		common.SendData(ack, targetIP, config.NodeACKListenPort, "ack")
	//2) as leaf, receive data from root
	case config.DDULeaf:
		//role = config.Role_DDULeafP
		//received data
		delta := td.Buff
		oldBuff := common.ReadBlock(td.BlockID)
		newBuff := make([]byte, config.ChunkSize)
		if config.ECMode == "RS" {
			row := common.GetParityIDFromIP(td.ToIP)
			col := td.BlockID - (td.BlockID/config.K)*config.K
			factor := config.RS.GenMatrix[row*config.K+col]
			for i := 0; i < len(delta); i++ {
				newBuff[i] = config.Gfmul(factor, delta[i]) ^ oldBuff[i]
			}
		}else { //XOR
			for i := 0; i < len(delta); i++ {
				newBuff[i] = delta[i] ^ oldBuff[i]
			}
		}
		common.WriteBlock(td.BlockID, newBuff)
		fmt.Printf("CMD_DDU: leafP update success!\n")
	//PDU mode, receive data from rootD
	case config.PDU:
		delta := td.Buff
		oldBuff := common.ReadBlock(td.BlockID)
		newBuff := make([]byte, config.ChunkSize)
		if config.ECMode == "RS" {
			row := common.GetParityIDFromIP(td.ToIP)
			col := td.BlockID - (td.BlockID/config.K)*config.K
			factor := config.RS.GenMatrix[row*config.K+col]
			for i := 0; i < len(delta); i++ {
				newBuff[i] = config.Gfmul(factor, delta[i]) ^ oldBuff[i]
			}
		}else { //XOR
			for i := 0; i < len(delta); i++ {
				newBuff[i] = delta[i] ^ oldBuff[i]
			}
		}
		common.WriteBlock(td.BlockID, newBuff)
		//return ack
		ack := &config.ACK{
			SID: td.SID,
			BlockID: td.BlockID,
		}
		common.SendData(ack, targetIP, config.NodeACKListenPort, "ack")

	}
}

func (p CAU) HandleReq(reqData config.ReqData)  {
	totalReqChunks = append(totalReqChunks, reqData)
	if len(totalReqChunks) >= maxBatchSize && !IsRunning {
		fmt.Printf("Starting cau update algorithm...\n")
		fmt.Printf("==================================\n")
		IsRunning = true
		curReqChunks := totalReqChunks[:maxBatchSize]
		totalReqChunks = totalReqChunks[maxBatchSize:]
		p.rackUpdate(curReqChunks)
		p.rackCompare(config.Racks[0], config.Racks[2])
		p.rackCompare(config.Racks[1], config.Racks[2])
	}
}

func (p CAU) HandleCMD(cmd config.CMD)  {
		p.DDU(cmd)
}

//R1 is a rack for datanode, R2 is a rack for paritynode
func (p CAU) rackCompare(R1 config.Rack, R2 config.Rack) {
	if R1.NumOfUpdates <= R2.NumOfUpdates {
		for stripeID, stripeBlocks := range R1.Stripes {
			fmt.Printf("CMD_DDU mode: handle Rack stripe %d...\n", stripeID)
			for _, blockID := range stripeBlocks {
				//将块信息发给root
				curNode := common.GetNodeID(blockID)
				curNodeIP := common.GetDataNodeIP(curNode)
				rootParityIP := common.GetRelatedParityIPs(blockID)[0] // 指定P0为rootParity（默认所有Parity都需要更新）
				toIPs := make([]string, 1)
				toIPs = append(toIPs, rootParityIP)
				cmd := config.CMD{
					Type:     config.CMD_DDU,
					StripeID: stripeID,
					BlockID:  blockID,
					ToIPs:    toIPs,
				}
				fmt.Printf("发送命令给 Node %d (%s)，使其将Block %d 发送给%s.\n", curNode, curNodeIP, blockID, rootParityIP)
				common.SendData(cmd, curNodeIP, config.NodeCMDListenPort, "")
				WaitingACKGroup[cmd.SID] = &config.WaitingACKItem{BlockID: blockID, RequiredACK: 1, SID: cmd.SID}
			}
		}
	} else {
		fmt.Printf(" i > j\n")
	}
}
/*******update R0~R2 with current update requests(curReqChunks)********/
func (p CAU) rackUpdate(curReqChunks  []config.ReqData)  {
	//update Rack
	for _, block := range curReqChunks {
		nodeID := common.GetNodeID(block.BlockID)
		stripeIndex := common.GetStripeID(block.BlockID)
		rackID := common.GetRackIDFromNode(nodeID) //获取chunk对应的rackID
		//if:stripe未出现; elseif:chunk未出现
		if _, ok := config.Racks[rackID].Stripes[stripeIndex]; !ok {
			config.Racks[rackID].NumOfUpdates++
			config.Racks[rackID].Stripes[stripeIndex] = append(config.Racks[rackID].Stripes[stripeIndex], block.BlockID)
			NumOfCurNeedUpdateBlocks++
		} else if arrays.Contains(config.Racks[rackID].Stripes[stripeIndex], block.BlockID) < 0 {
			config.Racks[rackID].NumOfUpdates++
			config.Racks[rackID].Stripes[stripeIndex] = append(config.Racks[rackID].Stripes[stripeIndex], block.BlockID)
			NumOfCurNeedUpdateBlocks++
		}
		//更新parity(出现过的就不更新了)
		//if _, ok := config.Racks[2].Stripes[stripeIndex]; !ok {
		//	config.Racks[2].NumOfUpdates += 3
		//	config.Racks[2].Stripes[stripeIndex] = append(config.Racks[2].Stripes[stripeIndex],
		//		p0ParityID, p0ParityID+1, p0ParityID+2)
		//}
	}
	fmt.Printf("this stripe we need update %d data blocks...\n", NumOfCurNeedUpdateBlocks)
	//printUpdatedStripes()
}

func (p CAU) DDU(cmd config.CMD)  {
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID)
	for _, ip := range cmd.ToIPs {
		sendData := config.TD{
			OPType:  config.DDURoot,
			BlockID: cmd.BlockID,
			Buff:    buff,
			ToIP:    ip,
			NextIPs: common.GetNeighborsIPs(common.GetRackID(ip), common.GetLocalIP()),
		}
		fmt.Printf("get cmd: CMD_DDU, data has been transferred to rootP (%s)\n", ip)
		common.SendData(sendData, sendData.ToIP, config.NodeCMDListenPort, "")
	}

}
func (p CAU) HandleACK(ack config.ACK)  {
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
func (p CAU) Clear()  {

}

func (p CAU)	RecordSIDAndReceiverIP(sid int, ip string)()  {

}