package main

import (
	"EC/common"
	"EC/config"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)



var role = config.UknownRole
var ackNum = 0
var neibors = (config.K+config.M)/config.M - 1
//var nodeIPForACK string
//var stringIDForACK int
func handleReq(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	targetIP := strings.Split(conn.RemoteAddr().String(),":")[0]

	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleUpdateReq:parityNode更新数据，解码出错: ", err)
	}

	fmt.Printf("handleReq: %v\n", td)

	switch td.OPType {

	//DDU模式，rootP接收到数据
	case config.DDURoot:

		role = config.DDURootPRole
		//nodeIPForACK = targetIP
		//stringIDForACK = td.StripeID
		//received data
		buff := td.Buff


		oldBuff := make([]byte, config.ChunkSize, config.ChunkSize)

		file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)
		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()
		index := td.StripeID
		file.ReadAt(oldBuff, int64(index*config.ChunkSize))

		//cau compute parity new value
		row := 0
		col := td.DataChunkID - (td.DataChunkID/config.K)*config.K
		factor := config.RS.GenMatrix[row*config.K+col]
		for i := 0; i < len(buff); i++ {
			buff[i] =  config.Gfmul(factor, buff[i]) ^ oldBuff[i]
		}
		//write to file
		file.Write(buff)

		fmt.Printf("DDU: rootP update success!\n")
		fmt.Printf("transfer data to other leafs...\n")

		//send update data to leafs, wait for ack
		for _, leafIP := range td.NextIPs {

			//send data to leaf
			data := &config.TD{
				OPType:      config.DDULeaf,
				Buff:        buff,
				DataChunkID: td.DataChunkID,
				ToIP: leafIP,
			}
			common.SendData(data, leafIP, config.ParityListenPort, "ack")
		}
		//return ack to datanode
		ack := config.Ack{
			ChunkID: td.DataChunkID,
			AckID: td.DataChunkID+1,
		}
		fmt.Printf("return the ack of chunk %d to client...\n", td.DataChunkID)

		common.SendData(ack, targetIP, config.NodeACKListenPort, "ack")

	//2) as leaf, receive data from root
	case config.DDULeaf:

		role = config.DDULeafPRole
		//received data
		buff := td.Buff

		oldBuff := make([]byte, config.ChunkSize, config.ChunkSize)

		file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)
		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()
		index := td.StripeID
		file.ReadAt(oldBuff, int64(index*config.ChunkSize))


		//cau compute parity new value
		row := common.GetParityIDFromIP(td.ToIP)
		col := td.DataChunkID - (td.DataChunkID/config.K)*config.K
		factor := config.RS.GenMatrix[row*config.K+col]
		for i := 0; i < len(buff); i++ {
			buff[i] =  config.Gfmul(factor, buff[i]) ^ oldBuff[i]
		}
		//write to file
		file.WriteAt(buff, int64(index*config.ChunkSize))
		fmt.Printf("DDU: leafP update success!\n")

		//return ack
		//ack := &config.ReqData{
		//	ChunkID: td.DataChunkID,
		//	AckID: td.DataChunkID+1,
		//}
		//common.SendData(ack, targetIP, config.ParityACKListenPort, "ack")

	//PDU mode, receive data from rootD
	case config.PDU:
		buff := td.Buff
		oldBuff := make([]byte, config.ChunkSize, config.ChunkSize)
		file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)

		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()

		index := td.StripeID
		file.ReadAt(oldBuff, int64(index*config.ChunkSize))

		//cau compute parity new value
		row := common.GetParityIDFromIP(td.ToIP)
		col := td.DataChunkID - (td.DataChunkID/config.K)*config.K
		factor := config.RS.GenMatrix[row*config.K+col]
		for i := 0; i < len(buff); i++ {
			buff[i] =  config.Gfmul(factor, buff[i]) ^ oldBuff[i]
		}
		//write to file
		file.WriteAt(buff, int64(index*config.ChunkSize))

		//return ack
		ack := &config.Ack{
			ChunkID: td.DataChunkID,
			AckID: td.DataChunkID+1,
		}
		common.SendData(ack, targetIP, config.NodeACKListenPort, "ack")
	}
}

//func handleACK(conn net.Conn) {
//	defer conn.Close()
//	dec := gob.NewDecoder(conn)
//
//	var ack config.Ack
//	err := dec.Decode(&ack)
//	if err != nil {
//		log.Fatal("parity decoded error: ", err)
//	}
//	ackNum++
//
//	//return stripe ack (stripe update finished)
//	if ackNum == neibors{
//		//return ack
//		ack := &config.Ack{
//			ChunkID: td.DataChunkID,
//			AckID: td.DataChunkID+1,
//		}
//		enc := gob.NewEncoder(conn)
//		err = enc.Encode(ack)
//		if err != nil {
//			fmt.Printf("parityNode : handleReq encode err:%v", err)
//			return
//		}
//	}
//
//	fmt.Printf("parity received chunk %d's ack：%d\n",ack.ChunkID, ack.AckID)
//
//}
func main() {
	config.InitNodesRacks()

	fmt.Printf("listening req in %s:%s\n", common.GetLocalIP(), config.ParityListenPort)
	l1, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.ParityListenPort)
	//l2, err := net.Listen("tcp", "localhost:"+config.ParityACKListenPort)

	if err != nil {
		log.Fatal("parity listen err: ", err)
	}
	//go listenACK(l2)
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

//func listenACK(listen net.Listener) {
//
//	defer listen.Close()
//	for {
//		conn, err := listen.Accept()
//		if err != nil {
//			fmt.Println("accept failed, err:%v", err)
//			continue
//		}
//		go handleACK(conn)
//	}
//}

