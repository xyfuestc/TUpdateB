package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"github.com/pkg/profile"
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

	log.Printf("初始化共享池...\n")
	config.InitBufferPool()

	log.Printf("收到来自 %s 的命令，设置当前算法设置为%s, 当前XOR的blockSize=%vMB，RS的blockSize=%vMB, UsingMulticast=%v.\n",
		common.GetConnIP(conn), config.CurPolicyStr[p.Type], config.BlockSize/config.Megabyte, config.RSBlockSize/config.Megabyte, p.Multicast)
}
func handleACK(conn net.Conn) {
	defer conn.Close()
	ack := common.GetACK(conn)
	schedule.GetCurPolicy().HandleACK(&ack)
}
func main() {
	defer profile.Start(profile.MemProfile, profile.MemProfileRate(1)).Stop()

	config.Init()
	log.Printf("listening td in %s:%s\n", common.GetLocalIP(), config.NodeTDListenPort)
	l1, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeTDListenPort)
	if err != nil {
		log.Fatal("listening td err: ", err)
	}
	log.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)
	l2, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeCMDListenPort)
	if err != nil {
		log.Fatal("listening cmd err: ", err)
	}
	log.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	l3, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeACKListenPort)
	if err != nil {
		log.Fatal("listening ack err: ", err)
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
	go listenCMD(l2)
	go listenACK(l3)
	go listenSettings(l4)
	go common.ListenMulticast(schedule.ReceiveCh)
	//go common.HandlingACK(schedule.ReceiveAck)
	go MsgSorter(schedule.ReceiveCh, schedule.ReceiveAck)
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
func MsgSorter(receive <-chan config.MTU, ackCh chan<- config.ACK) {
	for  {
		select {
		case message := <-receive:
			schedule.GetCurPolicy().RecordSIDAndReceiverIP(message.SID, message.FromIP)
			//log.Printf("记录ACKIP：sid: %v, fromIP: %v\n", message.SID, message.FromIP)
			//构造td
			td := &config.TD{
				SID:            message.SID,
				Buff:           message.Data,
				BlockID:        message.BlockID,
				MultiTargetIPs: message.MultiTargetIPs,
				FromIP:         message.FromIP,
				SendSize:       message.SendSize,
			}
			go schedule.GetCurPolicy().HandleTD(td)
			//log.Printf("MsgSorter：接收数据完成，执行HandleTD, td: sid: %v, sendSize: %v.\n",
			//																td.SID, td.SendSize)
		}
	}
}
//func MsgSorter(receive <-chan config.MTU, ackCh chan<- config.ACK)  {
//	countMap := map[int]int{}
//	sidBuffs := map[int][]byte{}
//	for  {
//		select {
//		case message := <-receive:
			//返回ack
			//ack := config.ACK{
			//	BlockID: message.BlockID,
			//	SID: message.SID,
			//	FragmentID:message.FragmentID,
			//}
			//ackCh <- ack
			//处理消息
			//common.PrintMessage(message)
			//if message.IsFragment == false { //不需要组包
			//	schedule.GetCurPolicy().RecordSIDAndReceiverIP(message.SID, message.FromIP)
			//	log.Printf("记录ACKIP：sid: %v, fromIP: %v\n", message.SID, message.FromIP)
			//	//构造td
			//	td := &config.TD{
			//		SID:            message.SID,
			//		Buff:           message.Data,
			//		BlockID:        message.BlockID,
			//		MultiTargetIPs: message.MultiTargetIPs,
			//		FromIP:         message.FromIP,
			//		SendSize:       message.SendSize,
			//	}
			//	go schedule.GetCurPolicy().HandleTD(td)
			//	log.Printf("MsgSorter：接收数据完成，执行HandleTD, td: sid: %v, sendSize: %v.\n", td.SID, td.SendSize)
				//ack := config.ACK{
				//	BlockID: message.BlockID,
				//	SID: message.SID,
				//	FragmentID:message.FragmentID,
				//}
				//ackCh <- ack
			//} else { //需要组包
				//if _, ok := countMap[message.SID]; !ok { //第一次收到，记录sid
				//	countMap[message.SID] = message.FragmentCount - 1
				//	log.Printf("MsgSorter：还需要接收%v个分片数据.\n", countMap[message.SID])
				//	schedule.GetCurPolicy().RecordSIDAndReceiverIP(message.SID, message.FromIP)
				//	sidBuffs[message.SID] = message.Data
				//} else if countMap[message.SID] > 0 { //组包
				//	countMap[message.SID]--
				//	log.Printf("MsgSorter：还需要接收%v个分片数据.\n", countMap[message.SID])
				//	sidBuffs[message.SID] = append(sidBuffs[message.SID], message.Data...)
				//	if countMap[message.SID] == 0 { //组包完成，合并之后处理td
				//		//构造td
				//		td := &config.TD{
				//			SID:            message.SID,
				//			Buff:           sidBuffs[message.SID],
				//			BlockID:        message.BlockID,
				//			MultiTargetIPs: message.MultiTargetIPs,
				//			FromIP:         message.FromIP,
				//			SendSize:       message.SendSize,
				//		}
				//		go schedule.GetCurPolicy().HandleTD(td)
				//		log.Printf("MsgSorter：分片组包完成，执行HandleTD.\n")
				//
				//		delete(countMap, message.SID)
				//		delete(sidBuffs, message.SID)
				//	}
				//}
			//}

//		}
//
//
//
//	}
//
//}


