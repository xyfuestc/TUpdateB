package main

import (
	"EC/common"
	"EC/config"
	"EC/policy"
	"encoding/gob"
	"fmt"
	"log"
	"net"
)

var RequestNum = 0
var ackNum = 0
var totalReqChunks = make([]config.MetaInfo, 0, 1000000)
var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var round = 0
var actualUpdatedBlocks = 0
//var bitMatrix = make(config.Matrix, 0, config.K * config.M * config.W * config.W)
func handleAck(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var ack config.Ack
	err := dec.Decode(&ack)
	if err != nil {
		log.Fatal("ms decoded error: ", err)
	}
	ackNum++
	fmt.Printf("ms received chunk %d's ack：%d\n",ack.ChunkID, ack.AckID)
	if ackNum == policy.NumOfCurNeedUpdateBlocks {
		actualUpdatedBlocks += policy.NumOfCurNeedUpdateBlocks
		fmt.Printf("CAU Round %d has been completed...\n", round)
		fmt.Printf("Now actual updated blocks : %d.\n", actualUpdatedBlocks)
		fmt.Printf("==================================\n")
		round++
		clearUpdates()
	}
}
func handleReq(conn net.Conn) {
	res := parseAndRecordReq(conn, config.ReqData{})
	req, _ := res.(config.ReqData)
	handleWithOPType(req, conn)
}
func handleWithOPType(req config.ReqData, conn net.Conn) {
	blockID := 0
	switch req.OPType {
	case config.UpdateReq:
		blockID = req.BlockID
		stripeID := req.StripeID
		relatedParityIPs := common.GetRelatedParityIPs(blockID)
		nodeID := blockID %config.K
		nodeIP := common.GetNodeIP(nodeID)

		metaInfo := &config.MetaInfo{
			StripeID:         stripeID,
			BlockID:          blockID,
			ChunkStoreIndex:  blockID,
			RelatedParityIPs: relatedParityIPs,
			BlockIP:          common.GetChunkIP(blockID),
			DataNodeID:       nodeID,
		}
		switch config.CurPolicy {
		case config.BASE:
			for _, ip := range relatedParityIPs {
				cmd := config.CMD{
					Type:     config.CMD_BASE,
					StripeID: stripeID,
					BlockID:  blockID,
					ToIP:     ip,
				}
				fmt.Printf("发送命令给 Node %d (%s)，使其将Block %d 发送给%v\n", nodeID, nodeIP, blockID, ip)
				common.SendData(cmd, nodeIP, config.NodeCMDListenPort, "")
			}
		case config.CAU:
			fmt.Printf("Round %d: starting cau update algorithm...\n", round)
			fmt.Printf("==================================\n")
			policy.CAU_Update(&totalReqChunks)

		case config.DPR_Forest:
		case config.T_Update:
			fmt.Printf("Round %d: starting T_Update algorithm...\n", round)
			fmt.Printf("==================================\n")
			tasks := policy.T_Update(blockID)
			for _, task := range tasks {
				cmd := config.CMD{BlockID: blockID, Type:config.CMD_TUpdate, FromIP: common.GetNodeIPFromNodeID(int(task.Start)),
					ToIP: common.GetNodeIPFromNodeID(int(task.End))}
				common.SendData(cmd, common.GetNodeIPFromNodeID(int(task.Start)), config.NodeCMDListenPort, "")
			}
			round++
		}
		totalReqChunks = append(totalReqChunks, *metaInfo)  //record total request blocks
	default:
		log.Println("handleReq req.OPType error")
	}

}

func parseAndRecordReq(conn net.Conn, i interface{}) interface{} {
	/****记录请求数据：+1****/
	RequestNum++

	switch i.(type) {
	case config.ReqData:
		/****解析接收数据****/
		defer conn.Close()
		dec := gob.NewDecoder(conn)

		var req config.ReqData
		err := dec.Decode(&req)
		if err != nil {
			fmt.Printf("decode error:%v\n", err)
		}

		return req

	default:
		log.Println("parseAndRecordReq error!")

	}
	return nil
}


func decodeReq(conn net.Conn, req * interface{}) {


}
func PrintGenMatrix(gm []byte)  {

	fmt.Printf("Generation Matrix : \n[")
	for i := 0; i < config.M; i++ {
		for j := 0; j < config.K; j++ {
			fmt.Printf("%d ", gm[i*config.K+j])

			if i==config.M-1 && j==config.K-1 {
				fmt.Printf("%d]", gm[i*config.K+j])
			}
		}
		fmt.Println()
	}
}


func clearUpdates() {

	fmt.Printf("clear all ranks info...\n")

	for _, rank := range config.Racks {
		rank.NumOfUpdates = 0
		rank.Stripes = make(map[int][]int)
	}

	ackNum = 0
	policy.IsRunning = false

	// 考虑如果用户请求metainfo已经结束，无法启动CAU算法，则在每轮更新结束之后，启动CAU。
	if len(totalReqChunks) >= config.MaxBatchSize && !policy.IsRunning {
		policy.NumOfCurNeedUpdateBlocks = 0
		policy.CAU_Update(&totalReqChunks)
	}
}
func printUpdatedStripes()  {
	var i = 0
	for _, rack := range config.Racks{
		fmt.Printf("rack %d = %v\n", i, rack.Stripes)
		i++
	}
}
func main() {
	/*init RS, nodes and racks*/
	config.Init()
	PolicyInit(config.CurPolicy)

	fmt.Printf("the ms is listening req: %s\n",config.MSListenPort) //8787
	l1, err := net.Listen("tcp", config.MSIP +":" + config.MSListenPort)
	fmt.Printf("the ms is listening ack: %s\n",config.MSACKListenPort)  //8201
	l2, err := net.Listen("tcp", config.MSIP+ ":" + config.MSACKListenPort)

	if err != nil {
		log.Fatal("ms listen err: ", err)
	}
	go listenACK(l2)
	listenReq(l1)
}
func listenReq(listen net.Listener) {

	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		handleReq(conn)
	}
}
func listenACK(listen net.Listener) {

	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		go handleAck(conn)
	}

}
func PolicyInit(x config.PolicyType)  {
	var p policy.Policy
	switch x {
	case config.BASE:
		p = policy.Base{}
	case config.CAU:
		p = policy.CAU{}
	case config.T_Update:
		p = policy.TUpdate{}
	case config.DPR_Forest:
		p = policy.Forest{}
	}
	p.Init()
}