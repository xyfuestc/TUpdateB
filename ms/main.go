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

func handleUpdateReq(conn net.Conn) {
	defer conn.Close()

	dec := gob.NewDecoder(conn)

	var req config.UpdateReqData
	err := dec.Decode(&req)
	if err != nil {
		fmt.Printf("decode error:%v", err)
	}

	RequestNum++

	chunkID := 0
	switch req.OPType {
	case config.UPDT_REQ:
		chunkID = req.LocalChunkID
		stripeID := chunkID / config.K
		relatedParities := config.GetRelatedParities(chunkID)

		nodeID := chunkID - (chunkID/config.K)*config.K
		metaInfo := &config.MetaInfo{
			StripeID:        stripeID,
			DataChunkID:     chunkID,
			ChunkStoreIndex: chunkID,
			RelatedParities: relatedParities,
			ChunkIP:         common.GetLocalIP(), //获取本地IP
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

		if len(totalReqChunks) >= config.MaxBatchSize {
			CAU_Update()
		}
	}


}
func initialize(k, m, w int) {
	fmt.Printf("初始化生成矩阵:k=%d, m=%d\n", k, m)
	r, _ := reedsolomon.New(k, m)
	config.RS = r

	//gm, err := reedsolomon.New(config.K, config.M)
	//if err != nil {
	//	log.Fatal("初始化矩阵：发生错误:", err)
	//}else{
	//	fmt.Printf("gm: %v\n", gm.)
	//}

	//bitMatrix := GenerateBitMatrix(r.GenMatrix,config.K, config.M, config.W)
	GenerateParityRelation(r.GenMatrix, config.CAU)
	fmt.Printf("初始化完成.\n")
	//fmt.Printf("bitMatrix=%v.\n",bitMatrix)
	fmt.Printf("RelationsInputP=%v.\n", RelationsInputP)
	fmt.Printf("RelationsInputD=%v.\n", RelationsInputD)
	/*******初始化rack*********/
	//initRack()

}

//cau algorithm
func CAU_Update() {

	//fmt.Printf("CAU_UPDATE:\n")
	curReqChunks = totalReqChunks[:100]
	totalReqChunks = totalReqChunks[100:]

	//for i := 0; i < len(curReqChunks); i++ {
	//	fmt.Printf(" %v", curReqChunks[i])
	//}
	//fmt.Println()
	//采用星型结构直接发送数据并更新
	for i := 0; i < len(curReqChunks); i++ {
		curChunk := curReqChunks[i]
		//nodeID = col
		nodeID := curChunk.DataChunkID - (curChunk.DataChunkID/config.K)*config.K
		row := curChunk.DataChunkID / config.K
		p0ParityID := row * config.M
		//如果chunk在Rack0里
		if _, ok := config.Rack0.Nodes[strconv.Itoa(nodeID)]; ok {

			//stripe未出现
			if _, ok := config.Rack0.Stripes[row]; !ok {
				config.Rack0.CurUpdateNum++
				config.Rack0.Stripes[row] = append(config.Rack0.Stripes[row], curChunk.DataChunkID)
				//chunk未出现
			} else if !common.IsContain(config.Rack0.Stripes[row], curChunk.DataChunkID) {
				config.Rack0.CurUpdateNum++
				config.Rack0.Stripes[row] = append(config.Rack0.Stripes[row], curChunk.DataChunkID)

			}

			//更新parity(出现过的就不更新了)
			if _, ok := config.Rack2.Stripes[row]; !ok {
				config.Rack2.CurUpdateNum += 3
				config.Rack2.Stripes[row] = append(config.Rack2.Stripes[row],
					p0ParityID, p0ParityID+1, p0ParityID+2)
			}
			//如果chunk在Rack1里
		} else if _, ok := config.Rack1.Nodes[strconv.Itoa(nodeID)]; ok {

			//stripe未出现
			if _, ok := config.Rack1.Stripes[row]; !ok {
				config.Rack1.CurUpdateNum++
				config.Rack1.Stripes[row] = append(config.Rack1.Stripes[row], curChunk.DataChunkID)
				//chunk未出现
			} else if !common.IsContain(config.Rack1.Stripes[row], curChunk.DataChunkID) {
				config.Rack1.CurUpdateNum++
				config.Rack1.Stripes[row] = append(config.Rack1.Stripes[row], curChunk.DataChunkID)

			}
			//更新parity
			if _, ok := config.Rack2.Stripes[row]; !ok {
				config.Rack2.CurUpdateNum += 3
				config.Rack2.Stripes[row] = append(config.Rack2.Stripes[row],
					p0ParityID, p0ParityID+1, p0ParityID+2)
			}

		}
	}

	//比较Rack0和Rack2：i < j，采用Data-delta Update，将data分别发送给一个rootParity，在由rootParity进行分发
	if config.Rack0.CurUpdateNum <= config.Rack2.CurUpdateNum {
		//分别对Rack0的不同的stripe进行处理
		for k, chunks := range config.Rack0.Stripes {

			fmt.Printf("DDU模式：处理 Rack0 stripe %d...\n", k)
			// 指定P0为rootParity（默认所有Parity都需要更新）
			rootParityIP := config.Rack2.Nodes["0"]
			//对于处于不同node的块进行处理
			for i := 0; i < len(chunks); i++ {
				//将块信息发给root
				curNode := chunks[i] - (chunks[i]/config.K)*config.K
				curNodeIP := common.GetIP(curNode)
				cmd := config.CMD{
					Type:        config.DataDeltaUpdate,
					StripeID:    k,
					DataChunkID: chunks[i],
					ToIP:        rootParityIP,
				}
				fmt.Printf("发送命令给Node%d，使其将Chunk%d发送给%s", curNode, chunks[i], rootParityIP)
				common.SendData(cmd, curNodeIP, config.CMDPort, "ack")
			}
		}
	} else {
		fmt.Printf(" i > j\n")
	}

	//比较Rack1和Rack2：i < j,采用Data-delta Update
	if config.Rack1.CurUpdateNum <= config.Rack2.CurUpdateNum {
		//分别对Rack0的不同的stripe进行处理
		for k, chunks := range config.Rack1.Stripes {

			fmt.Printf("DDU模式：处理 Rack1 stripe %d...\n", k)

			rootParityIP := config.Rack2.Nodes["0"]
			//对于处于不同node的块进行处理
			for i := 0; i < len(chunks); i++ {
				//将块发给root
				curNode := chunks[i] - (chunks[i]/config.K)*config.K
				curNodeIP := common.GetIP(curNode)
				cmd := config.CMD{
					Type:        config.DataDeltaUpdate,
					StripeID:    k,
					DataChunkID: chunks[i],
					ToIP:        rootParityIP,
				}
				fmt.Printf("发送命令给Node%d，使其将Chunk%d发送给%s", curNode, chunks[i], rootParityIP)
				common.SendData(cmd, curNodeIP, config.CMDPort, "ack")

			}
		}
	} else {
		fmt.Printf(" i > j\n")
	}

}

func clearUpdates() {
	config.Rack0.CurUpdateNum = 0
	config.Rack0.Stripes = make(map[int][]int)

	config.Rack1.CurUpdateNum = 0
	config.Rack1.Stripes = make(map[int][]int)

	config.Rack2.CurUpdateNum = 0
	config.Rack2.Stripes = make(map[int][]int)

	ackNum = 0
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
//	config.Rack1 = make(map[string]string)
//	config.Rack2 = make(map[string]string)
//	config.Rack3 = make(map[string]string)
//
//	for i := 1; i <= len(config.DataNodeIPs); i++ {
//		if i <= (config.K+config.M)/config.K {
//			config.Rack1[strconv.Itoa(i)]  = config.DataNodeIPs[i]
//			config.Rack3[strconv.Itoa(i)]  = config.ParityNodeIPs[i]
//		}else{
//			config.Rack2[strconv.Itoa(i)]  = config.DataNodeIPs[i]
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
	listenACK()
}

func listen() {
	initialize(config.K, config.M, config.W)
	//1.listen port:8977
	IP := fmt.Sprintf("%s:%d", config.MSIP, config.ListenPort)
	fmt.Println(IP)
	listen, err := net.Listen("tcp", IP)
	if err != nil {
		fmt.Printf("listen failed, err:%v", err)
		return
	}

	for {
		//2.wait for client
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		//3.handle client requests
		go handleUpdateReq(conn)



	}
}

func listenACK() {
	//1.create TCP connection
	IP := fmt.Sprintf("%s:%d", config.MSIP, config.ACKPort)
	listen, err := net.Listen("tcp", IP)
	if err != nil {
		fmt.Printf("listen failed, err:%v", err)
		return
	}
	
	for {
		//2.wait for ack
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		//3.handle ack
		go handleACK(conn)

	}
}

func handleACK(conn net.Conn) {
	defer conn.Close()

	dec := gob.NewDecoder(conn)

	var ack config.ACKData
	err := dec.Decode(&ack)
	if err != nil {
		log.Fatal("decode error, ", err)
	}

	ackNum++

	fmt.Printf("已收到ack：%d\n", ackNum)

	if ackNum == config.Rack0.CurUpdateNum+config.Rack1.CurUpdateNum {
		fmt.Printf("批处理完成...\n")
		//清空当前更新
		clearUpdates()
	}

}
