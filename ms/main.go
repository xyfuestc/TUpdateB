package main

import (
	"EC/common"
	"EC/config"
	"encoding/gob"
	"fmt"
	"github.com/templexxx/reedsolomon"
	"log"
	"net"
	"strconv"
)

var RelationsInputP = make([][]int, config.M*config.W, config.M*config.W)
var RelationsInputD = make([][]int, config.K*config.W, config.K*config.W)

var RequestNum = 0

var ackNum = 0

var totalReqChunks = make([]config.MetaInfo, 0, 1000000)

var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
func handleAck(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var req config.ReqData
	err := dec.Decode(&req)
	if err != nil {
		fmt.Printf("decode error:%v\n", err)
	} else {
		fmt.Printf("req : %v\n", req)
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
		enc := gob.NewEncoder(conn)
		err = enc.Encode(metaInfo)
		if err != nil {
			fmt.Printf("encode err:%v", err)
			return
		} else {
			totalReqChunks = append(totalReqChunks, *metaInfo)
		}
		// start CAU when achieve the threshold (100)
		if len(totalReqChunks) >= config.MaxBatchSize {
			CAU_Update()
		}
		//handle ack
		//case config.ACK:
		//	ackNum++
		//
		//	fmt.Printf("received ack：%d\n", ackNum)
		//
		//	if ackNum == config.Racks[0].CurUpdateNum+config.Racks[1].CurUpdateNum {
		//		fmt.Printf("batch updates have been completed...\n")
		//
		//		clearUpdates()
		//	}

	}
}

func handleReq(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var req config.ReqData
	err := dec.Decode(&req)
	if err != nil {
		fmt.Printf("decode error:%v\n", err)
	} else {
		fmt.Printf("req : %v\n", req)
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
		enc := gob.NewEncoder(conn)
		err = enc.Encode(metaInfo)
		if err != nil {
			fmt.Printf("encode err:%v", err)
			return
		} else {
			totalReqChunks = append(totalReqChunks, *metaInfo)
		}
		// start CAU when achieve the threshold (100)
		if len(curReqChunks) >= config.MaxBatchSize {
			CAU_Update()
		}
		//handle ack
		//case config.ACK:
		//	ackNum++
		//
		//	fmt.Printf("received ack：%d\n", ackNum)
		//
		//	if ackNum == config.Racks[0].CurUpdateNum+config.Racks[1].CurUpdateNum {
		//		fmt.Printf("batch updates have been completed...\n")
		//
		//		clearUpdates()
		//	}

	}

}
func initialize(k, m, w int) {
	fmt.Printf("Starting metainfo server...\n")
	fmt.Printf("initial parameters: k=%d, m=%d\n", k, m)
	r, _ := reedsolomon.New(k, m)
	config.RS = r

	//fmt.Printf("r : %v\n",r)

	//gm, err := reedsolomon.New(config.K, config.M)
	//if err != nil {
	//	log.Fatal("初始化矩阵：发生错误:", err)
	//}else{
	//	fmt.Printf("gm: %v\n", gm.)
	//}

	//bitMatrix := GenerateBitMatrix(r.GenMatrix,config.K, config.M, config.W)
	GenerateParityRelation(r.GenMatrix, config.CAU)
	fmt.Printf("initialization is finished.\n")

	PrintGenMatrix(r.GenMatrix)

	//init Nodes and Racks
	var start  = config.StartIP
	for g := 0; g < len(config.DataNodeIPs); g++ {
		strIP := config.BaseIP + strconv.FormatInt(int64(start), 10)
		config.DataNodeIPs[g] = strIP
		start++
	}

	for g := 0; g < len(config.ParityNodeIPs); g++ {
		strIP := config.BaseIP + strconv.FormatInt(int64(start), 10)
		config.ParityNodeIPs[g] = strIP
		start++
	}

	start = config.StartIP

	for g := 0; g < len(config.Racks); g++ {
		strIP1 := config.BaseIP + strconv.FormatInt(int64(start), 10)
		strIP2 := config.BaseIP + strconv.FormatInt(int64(start+1), 10)
		strIP3 := config.BaseIP + strconv.FormatInt(int64(start+2), 10)
		config.Racks[g] = config.Rack{
			Nodes:        map[string]string{"0": strIP1, "1": strIP2, "2": strIP3},
			NodeNum:      3,
			CurUpdateNum: 0,
			Stripes:      map[int][]int{},
			GateIP:       "",
		}
		start++
	}


	//fmt.Printf("bitMatrix=%v.\n",bitMatrix)
	//fmt.Printf("RelationsInputP=%v.\n", RelationsInputP)
	//fmt.Printf("RelationsInputD=%v.\n", RelationsInputD)
	/*******初始化rack*********/
	//initRack()

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

	fmt.Printf("starting cau update algorithm...\n")
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
			rootParityIP := R2.Nodes["0"]

			//lenI := len(chunks)

			//handle stripe[i][j]
			for i := 0; i < len(chunks); i++ {
				//将块信息发给root
				curNode := chunks[i] - (chunks[i] / config.K) * config.K
				curNodeIP := common.GetNodeIP(curNode)
				cmd := config.TD{
					OPType:      config.DDU,
					StripeID:    row,
					DataChunkID: chunks[i],
					ToIP:        rootParityIP,
				}
				fmt.Printf("发送命令给 Node %d，使其将Chunk %d 发送给%s\n", curNode, chunks[i], rootParityIP)
				res := common.SendData(cmd, curNodeIP, config.NodeListenPort, "ack")
				ack, ok := res.(config.ReqData)
				if ok {
					fmt.Printf("成功更新数据块：%d\n", ack.ChunkID)
					common.SendData(ack, config.MSIP, config.MSListenPort, "")
				} else {
					log.Fatal("client updateData 解码出错!")
				}
			}
		}
	} else {
		fmt.Printf(" i > j\n")
	}
}

func clearUpdates() {
	config.Racks[0].CurUpdateNum = 0
	config.Racks[0].Stripes = make(map[int][]int)

	config.Racks[1].CurUpdateNum = 0
	config.Racks[1].Stripes = make(map[int][]int)

	config.Racks[2].CurUpdateNum = 0
	config.Racks[2].Stripes = make(map[int][]int)

	ackNum = 0
}
/*******update R0~R2 with current update requests(curReqChunks)********/
func rackUpdate()  {

	//update Rack
	for i := 0; i < len(curReqChunks); i++ {
		curChunk := curReqChunks[i]
		//nodeID = col
		nodeID := curChunk.DataChunkID - (curChunk.DataChunkID/config.K)*config.K
		row := curChunk.DataChunkID / config.K
		p0ParityID := row * config.M
		//如果chunk在Rack0里
		if _, ok := config.Racks[0].Nodes[strconv.Itoa(nodeID)]; ok {

			//stripe未出现
			if _, ok := config.Racks[0].Stripes[row]; !ok {
				config.Racks[0].CurUpdateNum++
				config.Racks[0].Stripes[row] = append(config.Racks[0].Stripes[row], curChunk.DataChunkID)
				//chunk未出现
			} else if !common.IsContain(config.Racks[0].Stripes[row], curChunk.DataChunkID) {
				config.Racks[0].CurUpdateNum++
				config.Racks[0].Stripes[row] = append(config.Racks[0].Stripes[row], curChunk.DataChunkID)

			}

			//更新parity(出现过的就不更新了)
			if _, ok := config.Racks[2].Stripes[row]; !ok {
				config.Racks[2].CurUpdateNum += 3
				config.Racks[2].Stripes[row] = append(config.Racks[2].Stripes[row],
					p0ParityID, p0ParityID+1, p0ParityID+2)
			}
			//如果chunk在Rack1里
		} else if _, ok := config.Racks[1].Nodes[strconv.Itoa(nodeID)]; ok {

			//stripe未出现
			if _, ok := config.Racks[1].Stripes[row]; !ok {
				config.Racks[1].CurUpdateNum++
				config.Racks[1].Stripes[row] = append(config.Racks[1].Stripes[row], curChunk.DataChunkID)
				//chunk未出现
			} else if !common.IsContain(config.Racks[1].Stripes[row], curChunk.DataChunkID) {
				config.Racks[1].CurUpdateNum++
				config.Racks[1].Stripes[row] = append(config.Racks[1].Stripes[row], curChunk.DataChunkID)

			}
			//更新parity
			if _, ok := config.Racks[2].Stripes[row]; !ok {
				config.Racks[2].CurUpdateNum += 3
				config.Racks[2].Stripes[row] = append(config.Racks[2].Stripes[row],
					p0ParityID, p0ParityID+1, p0ParityID+2)
			}

		}
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

func GenerateParityRelation(gm []byte, strategy config.Strategy) {
	switch strategy {
	case config.CAU:

		col := config.K * config.W
		for i := 0; i < len(gm); i++ {
			if gm[i] > 0 {
				RelationsInputP[i/col] =
					append(RelationsInputP[i/col], i-(i/col*col))
			}
		}
		for i := 0; i < len(gm); i++ {
			if gm[i] > 0 {
				RelationsInputD[i-(i/col*col)] =
					append(RelationsInputD[i-(i/col*col)], i/col)
			}
		}

	}

}

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
	listen()
}

func listen() {
	initialize(config.K, config.M, config.W)
	//1.listen port:8977
	config.MSIP = "127.0.0.1"

	listenAddress := fmt.Sprintf("%s:%d", config.MSIP, config.MSListenPort)
	//ackListenAddress := fmt.Sprintf("%s:%d", config.MSIP, config.MSACKListenPort)


	listen, err := net.Listen("tcp", listenAddress)
	//listenAck, err := net.Listen("tcp", ackListenAddress)
	if err != nil {
		fmt.Printf("listen failed, err:%v", err)
		return
	}

	for {
		//2.wait for client
		conn, err := listen.Accept()
		//connAck, err := listenAck.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		//3.handle client requests
		//go handleAck(connAck) //create a new thread
		handleReq(conn)
	}
}



