package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"log"
	"net"
)
var connections []net.Conn

func handleCMD(conn net.Conn)  {
	defer conn.Close()
	cmd := common.GetCMD(conn)
	schedule.GetCurPolicy().RecordSIDAndReceiverIP(cmd.SID, common.GetConnIP(conn))
	fmt.Printf("收到来自 %s 的命令: 将 sid: %d, block: %d 的更新数据发送给 %v.\n", common.GetConnIP(conn), cmd.SID, cmd.BlockID, cmd.ToIPs)
	schedule.GetCurPolicy().HandleCMD(&cmd)
}
func handleTD(conn net.Conn)  {
	defer conn.Close()
	td := common.GetTD(conn)
	log.Printf("收到来自 %s 的TD，sid: %d, blockID: %d.\n", common.GetConnIP(conn), td.SID, td.BlockID)
	schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, common.GetConnIP(conn))
	schedule.GetCurPolicy().HandleTD(&td)
}
func setPolicy(conn net.Conn)  {
	defer conn.Close()
	p := common.GetPolicy(conn)
	schedule.SetPolicy(config.PolicyType(p.Type))
	config.NumOfMB = p.NumOfMB
	log.Printf("收到来自 %s 的命令，设置当前算法设置为%s, 当前XOR的blockSize=%vMB，RS的blockSize=%vMB.\n",
		common.GetConnIP(conn), config.CurPolicyStr[p.Type], config.BlockSize, config.RSBlockSize)
}
func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(&ack)
}
func main() {
	config.Init()
	fmt.Printf("listening td in %s:%s\n", common.GetLocalIP(), config.NodeTDListenPort)
	l1, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeTDListenPort)
	if err != nil {
		log.Fatal("listening td err: ", err)
	}
	fmt.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)
	l2, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeCMDListenPort)
	if err != nil {
		log.Fatal("listening cmd err: ", err)
	}
	fmt.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	l3, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeACKListenPort)
	if err != nil {
		log.Fatal("listening ack err: ", err)
	}

	fmt.Printf("listening settings in %s:%s\n", common.GetLocalIP(), config.NodeSettingsListenPort)
	l4, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeSettingsListenPort)
	if err != nil {
		fmt.Printf("listening settings failed, err:%v\n", err)
		return
	}
	//清除连接
	defer func() {
		for _, conn := range connections {
			conn.Close()
		}
	}()
	go listenCMD(l2)
	go listenACK(l3)
	go listenSettings(l4)
	listenTD(l1)

}
func listenACK(listen net.Listener) {
	//defer listen.Close()
	for {
		conn, e := listen.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}
		go handleACK(conn)
		connections = append(connections, conn)
		if len(connections)%100 == 0 {
			log.Printf("total number of connections: %v", len(connections))
		}
	}
}
func listenCMD(listen net.Listener) {
	//defer listen.Close()
	for {
		//等待客户端连接
		conn, e := listen.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}
		go handleCMD(conn)
		connections = append(connections, conn)
		if len(connections)%100 == 0 {
			log.Printf("total number of connections: %v", len(connections))
		}
	}
}
func listenTD(listen net.Listener) {
	//defer listen.Close()
	for {
		//等待客户端连接
		conn, e := listen.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}
		go handleTD(conn)
		connections = append(connections, conn)
		if len(connections)%100 == 0 {
			log.Printf("total number of connections: %v", len(connections))
		}
	}
}
func listenSettings(listen net.Listener) {
	//defer listen.Close()
	for {
		//等待客户端连接
		conn, e := listen.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}
		go setPolicy(conn)
		connections = append(connections, conn)
	}
}
