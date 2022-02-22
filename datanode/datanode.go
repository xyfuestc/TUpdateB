package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"log"
	"net"
)
var connections []net.Conn
func handleCMD(conn net.Conn)  {
	defer conn.Close()
	cmd := common.GetCMD(conn)
	schedule.GetCurPolicy().RecordSIDAndReceiverIP(cmd.SID, common.GetConnIP(conn))
	log.Printf("收到来自 %s 的命令: 将 sid: %d, block: %d 的更新数据发送给 %v.\n", common.GetConnIP(conn), cmd.SID, cmd.BlockID, cmd.ToIPs)
	schedule.GetCurPolicy().HandleCMD(&cmd)
}
func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(&ack)
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
	config.BlockSize = p.NumOfMB * config.Megabyte
	config.RSBlockSize = p.NumOfMB * config.Megabyte * config.W
	log.Printf("收到来自 %s 的命令，设置当前算法设置为%s, 当前blockSize=%vMB.\n",
		common.GetConnIP(conn), config.CurPolicyStr[p.Type], config.BlockSize/config.Megabyte)
}


func main() {
	config.Init()

	log.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)
	l1, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeCMDListenPort)
	if err != nil {
		log.Printf("listening cmd failed, err:%v\n", err)
		return
	}
	log.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	l2, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeACKListenPort)
	if err != nil {
		log.Printf("listening ack failed, err:%v\n", err)
		return
	}
	log.Printf("listening td in %s:%s\n", common.GetLocalIP(), config.NodeTDListenPort)
	l3, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeTDListenPort)
	if err != nil {
		log.Printf("listening ack failed, err:%v\n", err)
		return
	}
	log.Printf("listening settings in %s:%s\n", common.GetLocalIP(), config.NodeSettingsListenPort)
	l4, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeSettingsListenPort)
	if err != nil {
		log.Printf("listening settings failed, err:%v\n", err)
		return
	}
	//清除连接
	defer func() {
		for _, conn := range connections {
			conn.Close()
		}
	}()

	go listenTD(l3)
	go listenACK(l2)
	go listenSettings(l4)
	//go timeout()
	//go common.ListenACK(schedule.ReceiveAck)
	go common.Multicast(schedule.SendCh)
	listenCMD(l1)
}
func listenCMD(listen net.Listener) {
	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()
		if err != nil {
			log.Printf("accept failed, err:%v\n", err)
			continue
		}
		go handleCMD(conn)
		connections = append(connections, conn)
		if len(connections)%100 == 0 {
			log.Printf("total number of connections: %v", len(connections))
		}
	}
}
func listenACK(listen net.Listener) {
	defer listen.Close()
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
		go handleACK(conn)
		connections = append(connections, conn)
		if len(connections)%100 == 0 {
			log.Printf("total number of connections: %v", len(connections))
		}
	}
}
func listenTD(listen net.Listener) {
	defer listen.Close()
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
	defer listen.Close()
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




