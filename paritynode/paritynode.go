package paritynode

import (
	"EC/common"
	"EC/config"
	"EC/ms"
	"encoding/gob"
	"fmt"
	"log"
	"net"
)



var role = config.Role_Uknown
var ackNum = 0
var neibors = (config.K+config.M)/config.M - 1
//var nodeIPForACK string
//var stringIDForACK int
var SidCounter = 0
var WaitingACKGroup = make(map[int]config.WaitingACKItem)

func handleReq(conn net.Conn) {
	td := common.GetTD(conn)
	ms.GetCurPolicy().HandleTD(td)
}
func handleCMD(conn net.Conn)  {
	cmd := GetCMD(conn)
	ms.GetCurPolicy().HandleCMD(cmd)
}
func GetCMD(conn net.Conn) config.CMD  {
	defer conn.Close()
	//decode the req
	dec := gob.NewDecoder(conn)
	var cmd config.CMD
	err := dec.Decode(&cmd)
	if err != nil {
		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
	}
	return cmd
}

//func handleACK(conn net.Conn) {
//	defer conn.Close()
//	dec := gob.NewDecoder(conn)
//
//	var ack config.ACK
//	err := dec.Decode(&ack)
//	if err != nil {
//		log.Fatal("parity decoded error: ", err)
//	}
//	ackNum++
//
//	//return stripe ack (stripe update finished)
//	if ackNum == neibors{
//		//return ack
//		ack := &config.ACK{
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

