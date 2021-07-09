package main

import (
	"EC/common"
	"EC/config"
	cau "EC/policy"
	"encoding/gob"
	"fmt"
	"log"
	"net"
)

//handle req
func handleReq(conn net.Conn) {
	//targetIP := strings.Split(conn.RemoteAddr().String(),":")[0]

	defer conn.Close()
	//decode the req
	dec := gob.NewDecoder(conn)
	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
	}

	switch td.OPType {
	case config.UpdateReq:
		cau.HandleReq(td)
	}
}
func handleCMD(conn net.Conn)  {
	defer conn.Close()
	//decode the req
	dec := gob.NewDecoder(conn)
	var cmd config.CMD
	err := dec.Decode(&cmd)
	if err != nil {
		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
	}

	switch cmd.Type {
	case config.CMD_DDU:
		cau.DDU(cmd)
	case config.CMD_BASE:
		deltaBuff := common.RandWriteBlockAndRetDelta(cmd.BlockID)
		td := &config.TD{
			OPType:  config.OP_BASE,
			Buff:    deltaBuff,
			BlockID: cmd.BlockID,
			ToIP:    cmd.ToIP,
		}
		fmt.Printf("send data to paritynode:  %s\n", td.ToIP)
		common.SendData(td, td.ToIP, config.NodeListenPort, "ack")
	}
}
func handleACK(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var ack config.Ack
	err := dec.Decode(&ack)
	if err != nil {
		log.Fatal("ms decoded error: ", err)
	}
	fmt.Printf("datanode received chunk %d's ack：%d\n",ack.ChunkID, ack.AckID)

	common.SendData(ack, config.MSIP, config.MSACKListenPort, "ack")
}
func TestSpeedInRack()  {
	dataStr := common.RandStringBytesMask(config.ChunkSize)
	dataBytes := []byte(dataStr)
	td := config.TD{Buff:dataBytes}
	common.SendData(td, "192.168.1.174", config.NodeListenPort, "")
}
func main() {
	TestSpeedInRack()
	//config.Init()
	//
	//fmt.Printf("listening req in %s:%s\n",common.GetLocalIP(), config.NodeListenPort)
	//l1, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeListenPort)
	//if err != nil {
	//	fmt.Printf("listenReq failed, err:%v\n", err)
	//	return
	//}
	//
	//fmt.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)
	//l2, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeCMDListenPort)
	//if err != nil {
	//	fmt.Printf("listenCMD failed, err:%v\n", err)
	//	return
	//}
	//
	//fmt.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	//l3, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeACKListenPort)
	//if err != nil {
	//	fmt.Printf("listenACK failed, err:%v\n", err)
	//	return
	//}
	//
	//go listenCMD(l2)
	//go listenACK(l3)
	//listenReq(l1)

}
func listenReq(listen net.Listener) {

	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()
		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		handleReq(conn)


	}
}
func listenCMD(listen net.Listener) {

	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()

		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		go handleCMD(conn)

	}
}
func listenACK(listen net.Listener) {


	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()

		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		go handleACK(conn)

	}

}




