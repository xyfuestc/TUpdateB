package main

import (
	"EC/common"
	"EC/config"
	"encoding/gob"
	"fmt"
	"log"
	"net"
)

var RelationsInputP = make([][]int, config.M*config.W, config.M*config.W)
var RelationsInputD = make([][]int, config.K*config.W, config.K*config.W)
var RequestNum = 0

var ackNum = 0
var totalReqChunks = make([]config.MetaInfo, 0, 1000000)
var isRunning = false   //标志是否正在执行CAU，若为false，则可以执行新的CAU；否则，不能执行
var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var curNeedUpdateBlocks = 0
var round = 0
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

	if ackNum == curNeedUpdateBlocks {
		fmt.Printf("cau update has been completed...\n")
		clearUpdates()
	}
}
func handleReq(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var req config.ReqData
	err := dec.Decode(&req)
	if err != nil {
		fmt.Printf("decode error:%v\n", err)
	}

	RequestNum++

	chunkID := 0
	switch req.OPType {
	//handle client update, return the specific chunk's metainfo
	case config.UpdateReq:
		chunkID = req.ChunkID
		stripeID := chunkID / config.K
		relatedParities := config.GetRelatedParities(chunkID)

		nodeID := chunkID - (chunkID/config.K)*config.K
		metaInfo := &config.MetaInfo{
			StripeID:        stripeID,
			DataChunkID:     chunkID,
			ChunkStoreIndex: chunkID,
			RelatedParities: relatedParities,
			ChunkIP:         common.GetChunkIP(chunkID),
			DataNodeID:      nodeID,
		}
		fmt.Printf("return the metainfo of chunk %d.\n", chunkID)
		enc := gob.NewEncoder(conn)
		err = enc.Encode(metaInfo)
		if err != nil {
			fmt.Printf("encode err:%v", err)
			return
		} else {
			totalReqChunks = append(totalReqChunks, *metaInfo)
		}
		// start CAU when achieves the threshold (100) and current stripe is not finished.
		if len(totalReqChunks) >= config.MaxBatchSize && !isRunning {
			CAU_Update()
		}
	}
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

//cau algorithm
func CAU_Update() {


	isRunning = true

	fmt.Printf("Round %d: starting cau update algorithm...\n", round)
	curReqChunks = totalReqChunks[:100]
	totalReqChunks = totalReqChunks[100:]

	// 1.rack update
	rackUpdate()
	// 2.rack updates compare:
	// 1)i < j, use Data-Delta Update(DDU); 2)else, use PDU
	rackCompare(config.Racks[0], config.Racks[2])
	rackCompare(config.Racks[1], config.Racks[2])

}
//R1 is a rack for datanode, R2 is a rack for paritynode
func rackCompare(R1 config.Rack, R2 config.Rack) {
	if R1.CurUpdateNum <= R2.CurUpdateNum {
		//handle stripe[i]
		for row, chunks := range R1.Stripes {

			fmt.Printf("DDU mode: handle Rack stripe %d...\n", row)
			// 指定P0为rootParity（默认所有Parity都需要更新）
			rootParityIP := R2.Nodes[0]

			fmt.Printf("rootP IP: %s\n", rootParityIP)

			//lenI := len(chunks)

			//handle stripe[i][j]
			for i := 0; i < len(chunks); i++ {
				//将块信息发给root
				curNode := chunks[i] - (chunks[i] / config.K) * config.K
				curNodeIP := common.GetNodeIP(curNode)
				cmd := config.CMD{
					Type:      config.DDU,
					StripeID:    row,
					DataChunkID: chunks[i],
					ToIP:        rootParityIP,
				}
				fmt.Printf("发送命令给 Node %d (%s)，使其将Chunk %d 发送给%s\n", curNode, curNodeIP,  chunks[i], rootParityIP)
				common.SendData(cmd, curNodeIP, config.NodeCMDListenPort, "")



			}
		}
	} else {
		fmt.Printf(" i > j\n")
	}
}

func clearUpdates() {

	fmt.Printf("clear all ranks info...\n")

	for _,rank := range config.Racks{
		rank.CurUpdateNum = 0
		rank.Stripes = make(map[int][]int)
	}

	ackNum = 0
	isRunning = false

	// 考虑如果用户请求metainfo已经结束，无法启动CAU算法，则在每轮更新结束之后，启动CAU。
	if len(totalReqChunks) >= config.MaxBatchSize && !isRunning {
		CAU_Update()
	}
}
/*******update R0~R2 with current update requests(curReqChunks)********/
func rackUpdate()  {


	//update Rack
	for i := 0; i < len(curReqChunks); i++ {
		curChunk := curReqChunks[i]
		//nodeID = col
		nodeID := curChunk.DataChunkID - (curChunk.DataChunkID/config.K)*config.K
		row := curChunk.DataChunkID / config.K
		p0ParityID := row * config.M    //指定rootParity = P[0]

		rackID := common.GetRackIDFromNode(nodeID) //获取chunk对应的rackID

		//if:stripe未出现; elseif:chunk未出现
		if _, ok := config.Racks[rackID].Stripes[row]; !ok {
			config.Racks[rackID].CurUpdateNum++
			config.Racks[rackID].Stripes[row] = append(config.Racks[rackID].Stripes[row], curChunk.DataChunkID)
			curNeedUpdateBlocks++
		} else if !common.IsContain(config.Racks[rackID].Stripes[row], curChunk.DataChunkID) {
			config.Racks[rackID].CurUpdateNum++
			config.Racks[rackID].Stripes[row] = append(config.Racks[rackID].Stripes[row], curChunk.DataChunkID)
			curNeedUpdateBlocks++
		}
		//更新parity(出现过的就不更新了)
		if _, ok := config.Racks[2].Stripes[row]; !ok {
			config.Racks[2].CurUpdateNum += 3
			config.Racks[2].Stripes[row] = append(config.Racks[2].Stripes[row],
				p0ParityID, p0ParityID+1, p0ParityID+2)
		}
	}
	fmt.Printf("this stripe we need update %d data blocks...\n", curNeedUpdateBlocks)
	printUpdatedStripes()

}

func printUpdatedStripes()  {

	var i = 0
	for _, rack := range config.Racks{
		fmt.Printf("rack %d = %v\n", i, rack.Stripes)
		i++
	}

}

//func GenerateParityRelation(gm []byte) {
//	//var relation = [][]byte
//	//relation := [][]int{}
//	//row := chunkID%config.K //row表示chunkID对应的stripeID，
//	//relation := make([][]int, config.M * config.W, config.M * config.W)
//	for i := 0; i < len(gm); i++ {
//		if gm[i] == 1 {
//			RelationsInputP[ i / ( config.K*config.W )] =
//				append(RelationsInputP[ i / ( config.K*config.W )],i-(i / ( config.K*config.W )*( config.K*config.W )))
//		}
//	}
//}

func getRelatedParityID(chunkID int) {

}

/*******初始化Rack**********/
//func initRack()  {
//	config.Racks[1] = make(map[string]string)
//	config.Racks[2] = make(map[string]string)
//	config.Rack3 = make(map[string]string)
//
//	for i := 1; i <= len(config.DataNodeIPs); i++ {
//		if i <= (config.K+config.M)/config.K {
//			config.Racks[1][strconv.Itoa(i)]  = config.DataNodeIPs[i]
//			config.Rack3[strconv.Itoa(i)]  = config.ParityNodeIPs[i]
//		}else{
//			config.Racks[2][strconv.Itoa(i)]  = config.DataNodeIPs[i]
//		}
//	}
//}

//func GenerateParityRelation(gm []byte, strategy config.Strategy) {
//	switch strategy {
//	case config.CAU:
//
//		col := config.K * config.W
//		for i := 0; i < len(gm); i++ {
//			if gm[i] > 0 {
//				RelationsInputP[i/col] =
//					append(RelationsInputP[i/col], i-(i/col*col))
//			}
//		}
//		for i := 0; i < len(gm); i++ {
//			if gm[i] > 0 {
//				RelationsInputD[i-(i/col*col)] =
//					append(RelationsInputD[i-(i/col*col)], i/col)
//			}
//		}
//
//	}
//
//}

func GenerateBitMatrix(matrix []byte, k, m, w int) []byte {

	bitMatrix := make([]byte, k*m*w*w)

	rowelts := k * w
	rowindex := 0

	for i := 0; i < m; i++ {
		colindex := rowindex
		for j := 0; j < k; j++ {
			elt := matrix[i*k+j]
			for x := 0; x < w; x++ {
				for l := 0; l < w; l++ {
					if (elt & (1 << l)) > 0 {
						bitMatrix[colindex+x+l*rowelts] = 1
					} else {
						bitMatrix[colindex+x+l*rowelts] = 0
					}
				}
				elt = config.Gfmul(elt, 2)
			}
			colindex += w
		}
		rowindex += rowelts * w
	}
	return bitMatrix

}

func main() {

	config.Init()

	fmt.Printf("the ms is listening req: %s\n",config.MSListenPort)
	l1, err := net.Listen("tcp", config.MSIP +":" + config.MSListenPort)
	fmt.Printf("the ms is listening ack: %s\n",config.MSACKListenPort)
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


