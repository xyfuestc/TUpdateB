package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"net"
)
func handleCMD(conn net.Conn)  {
	defer conn.Close()
	cmd := common.GetCMD(conn)
	schedule.GetCurPolicy().RecordSIDAndReceiverIP(cmd.SID, common.GetConnIP(conn))
	fmt.Printf("收到来自%s的命令...\n", common.GetConnIP(conn))
	schedule.GetCurPolicy().HandleCMD(cmd)
}
func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(ack)
}
func main() {
	config.Init()

	fmt.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)
	l1, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeCMDListenPort)
	if err != nil {
		fmt.Printf("listening cmd failed, err:%v\n", err)
		return
	}
	fmt.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	l2, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeACKListenPort)
	if err != nil {
		fmt.Printf("listening ack failed, err:%v\n", err)
		return
	}
	go listenACK(l2)
	listenCMD(l1)
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




