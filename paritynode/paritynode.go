package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"log"
	"net"
)
//func handleCMD(conn net.Conn)  {
//	cmd := common.GetCMD(conn)
//	schedule.GetCurPolicy().HandleCMD(cmd)
//}
func handleTD(conn net.Conn)  {
	td := common.GetTD(conn)
	log.Printf("received %s's td\n", common.GetConnIP(conn))
	schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, common.GetConnIP(conn))
	schedule.GetCurPolicy().HandleTD(td)
}
func handleACK(conn net.Conn) {
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(ack)
}
func main() {
	config.Init()

	l1, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeTDListenPort)
	if err != nil {
		log.Fatal("listening td err: ", err)
	}
	fmt.Printf("listening td in %s:%s\n", common.GetLocalIP(), config.NodeTDListenPort)

	//l2, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeCMDListenPort)
	//if err != nil {
	//	log.Fatal("listening cmd err: ", err)
	//}
	//fmt.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)

	l3, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeACKListenPort)
	if err != nil {
		log.Fatal("listening ack err: ", err)
	}
	fmt.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)

	//go listenCMD(l2)
	go listenACK(l3)
	listenTD(l1)

}
func listenACK(listen net.Listener) {
	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		go handleACK(conn)
	}
}
//func listenCMD(listen net.Listener) {
//	defer listen.Close()
//	for {
//		//等待客户端连接
//		conn, err := listen.Accept()
//		if err != nil {
//			fmt.Printf("accept failed, err:%v\n", err)
//			continue
//		}
//		go handleCMD(conn)
//	}
//}
func listenTD(listen net.Listener) {
	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()
		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		go handleTD(conn)
	}
}

