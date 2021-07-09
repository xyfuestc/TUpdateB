package policy

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
	"os"
)

type CAU struct {

}
func (p CAU) Init()  {

}


//var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var NumOfCurNeedUpdateBlocks = 0
var round = 0
var IsRunning = false   //check CAU is running or not
func CAU_Update(totalReqChunks * []config.MetaInfo) {
	if len(*totalReqChunks) >= config.MaxBatchSize && !IsRunning {
		IsRunning = true
		curReqChunks := (*totalReqChunks)[:config.MaxBatchSize]
		*totalReqChunks = (*totalReqChunks)[config.MaxBatchSize:]
		rackUpdate(curReqChunks)
		rackCompare(config.Racks[0], config.Racks[2])
		rackCompare(config.Racks[1], config.Racks[2])
	}
}
//R1 is a rack for datanode, R2 is a rack for paritynode
func rackCompare(R1 config.Rack, R2 config.Rack) {
	if R1.NumOfUpdates <= R2.NumOfUpdates {
		for stripeID, stripeBlocks := range R1.Stripes {
			fmt.Printf("CMD_DDU mode: handle Rack stripe %d...\n", stripeID)
			for _, block := range stripeBlocks {
				//将块信息发给root
				curNode := common.GetNodeID(block)
				curNodeIP := common.GetNodeIP(curNode)
				rootParityIP := common.GetRelatedParityIPs(block)[0] // 指定P0为rootParity（默认所有Parity都需要更新）
				cmd := config.CMD{
					Type:     config.CMD_DDU,
					StripeID: stripeID,
					BlockID:  block,
					ToIP:     rootParityIP,
				}
				fmt.Printf("发送命令给 Node %d (%s)，使其将Block %d 发送给%s.\n", curNode, curNodeIP, block, rootParityIP)
				common.SendData(cmd, curNodeIP, config.NodeCMDListenPort, "")
			}
		}
	} else {
		fmt.Printf(" i > j\n")
	}
}
/*******update R0~R2 with current update requests(curReqChunks)********/
func rackUpdate(curReqChunks  []config.MetaInfo)  {
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

func HandleReq(td config.TD)  {
	buff := td.Buff
	file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)
	//1.打开文件后，光标默认在文件开头。
	if err != nil {
		fmt.Printf("打开文件出错：%v\n", err)
		return
	}
	defer file.Close()
	index := td.StripeID
	file.Seek(int64((index-1)*config.ChunkSize), 0)
	file.Write(buff)

	ack := &config.ReqData{
		BlockID: td.BlockID,
		AckID:   td.BlockID + 1, //ackID=chunkID+1
	}
	fmt.Printf("The datanode updates success for chunk %d\n", td.BlockID)
	fmt.Printf("Return the ack of block %d.\n", td.BlockID)

	common.SendData(ack, config.ClientIP, config.ClientACKListenPort, "ack")

}

func DDU(cmd config.CMD)  {
	buff := common.RandWriteBlockAndRetDelta(cmd.BlockID)
	sendData := config.TD{
		OPType:  config.DDURoot,
		BlockID: cmd.BlockID,
		Buff:    buff,
		ToIP: cmd.ToIP,
		NextIPs: common.GetNeighborsIPs(common.GetRackID(cmd.ToIP), common.GetLocalIP()),
	}
	fmt.Printf("get cmd: CMD_DDU, data has been transferred to rootP (%s)\n", cmd.ToIP)
	common.SendData(sendData, sendData.ToIP, config.ParityListenPort, "")
}