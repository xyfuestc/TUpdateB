package main

import (
	"EC/common"
	"EC/config"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"strings"
)



var role = config.Role_Uknown
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

	switch td.OPType {
	case config.OP_BASE:
		common.WriteBlock(td.BlockID, td.Buff)
		//return ack
		ackNum++
		ack := &config.Ack{
			ChunkID: td.BlockID,
			AckID: ackNum+1,
		}
		common.SendData(ack, targetIP, config.NodeACKListenPort, "ack")
	//DDU模式，rootP接收到数据
	case config.DDURoot:
		role = config.Role_DDURootP
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
			common.SendData(data, leafIP, config.ParityListenPort, "")
		}
		ackNum++
		ack := &config.Ack{
			ChunkID: td.BlockID,
			AckID: ackNum+1,
		}
		fmt.Printf("return the ack of chunk %d to client...\n", td.BlockID)
		common.SendData(ack, targetIP, config.NodeACKListenPort, "ack")
	//2) as leaf, receive data from root
	case config.DDULeaf:
		role = config.Role_DDULeafP
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
		ackNum++
		ack := &config.Ack{
			ChunkID: td.BlockID,
			AckID: ackNum+1,
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
//			BlockID: td.blockID,
//			AckID: td.blockID+1,
//		}
//		enc := gob.NewEncoder(conn)
//		err = enc.Encode(ack)
//		if err != nil {
//			fmt.Printf("parityNode : handleReq encode err:%v", err)
//			return
//		}
//	}
//
//	fmt.Printf("parity received chunk %d's ack：%d\n",ack.BlockID, ack.AckID)
//
//}
func main() {
	config.Init()

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

