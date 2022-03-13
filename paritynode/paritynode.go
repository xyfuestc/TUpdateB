package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)
//var connections []net.Conn
//func handleCMD(conn net.Conn)  {
//
//}
//func handleTD(conn net.Conn)  {
//	defer conn.Close()
//	td := common.GetTD(conn)
//	log.Printf("收到来自 %s 的TD，sid: %d, blockID: %d.\n", common.GetConnIP(conn), td.SID, td.BlockID)
//	schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, common.GetConnIP(conn))
//	schedule.GetCurPolicy().HandleTD(&td)
//}
func setPolicy(conn net.Conn) {
	defer conn.Close()
	p := common.GetPolicy(conn)

	//检测结束
	if p.Type == -1 {
		log.Printf("收到结束信号...\n清空所有资源，继续等待命令...\n")
		finish()
		return
	}

	schedule.SetPolicy(config.Policies[p.Type])
	config.BlockSize = int(p.NumOfMB * config.Megabyte)
	config.RSBlockSize = int(p.NumOfMB*config.Megabyte) * config.W

	log.Printf("初始化共享池...\n")
	config.InitBufferPool()

	log.Printf("收到来自 %s 的命令，设置当前算法设置为%s, 当前blockSize=%.2fMB，RSBlockSize=%dMB.\n",
		common.GetConnIP(conn), config.Policies[p.Type], p.NumOfMB, config.RSBlockSize/config.Megabyte)

}

var done = make(chan bool)

//func handleACK(conn net.Conn) {
//	defer conn.Close()
//	ack := common.GetACK(conn)
//	schedule.GetCurPolicy().HandleACK(&ack)
//}
func main() {
	//defer profile.Start(profile.MemProfile, profile.MemProfileRate(1)).Stop()

	config.Init()

	//监听并接收ack，检测程序结束
	listenAndReceive(config.NumOfWorkers)

	//当发生意外退出时，安全释放所有资源
	registerSafeExit()


	for  {
		select {
		case <- done:
			return
		}
	}

}

func listenAndReceive(workers int) {

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

	go listenCMD(l2)
	go listenACK(l3)
	go listenSettings(l4)
	go listenTD(l1)
	go common.ListenMulticast(schedule.MulticastReceiveMTUCh)

	for i := 0; i < workers; i++ {
		//go common.HandlingACK(schedule.MulticastReceiveAckCh)
		go msgSorter(schedule.ReceivedAckCh, schedule.ReceivedTDCh, schedule.ReceivedCMDCh, schedule.MulticastReceiveMTUCh)
	}

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
		ack := common.GetACK(conn)
		schedule.ReceivedAckCh <- ack

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
		cmd := common.GetCMD(conn)
		schedule.GetCurPolicy().RecordSIDAndReceiverIP(cmd.SID, common.GetConnIP(conn))
		schedule.ReceivedCMDCh <- cmd
		//config.CMDBufferPool.Put(cmd)
		log.Printf("收到来自 %s 的命令: 将 sid: %d, block: %d 的更新数据发送给 %v.\n", common.GetConnIP(conn), cmd.SID, cmd.BlockID, cmd.ToIPs)

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
		td := common.GetTD(conn)
		conn.SetDeadline(time.Now().Add(200 * time.Millisecond))
		schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, common.GetConnIP(conn))
		schedule.ReceivedTDCh <- td
		//config.TDBufferPool.Put(td)
		log.Printf("收到来自 %s 的TD，sid: %d, blockID: %d.\n", common.GetConnIP(conn), td.SID, td.BlockID)

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
		setPolicy(conn)
	}
}
func msgSorter(receivedAckCh <-chan config.ACK, receivedTDCh <-chan config.TD, receivedCMDCh <-chan config.CMD, receivedMultiMTUCh <-chan config.MTU)  {
	for  {
		select {
		case ack := <-receivedAckCh:
			schedule.GetCurPolicy().HandleACK(&ack)

		case td := <-receivedTDCh:
			schedule.GetCurPolicy().HandleTD(&td)

		case cmd := <-receivedCMDCh:
			schedule.GetCurPolicy().HandleCMD(&cmd)

		case mtu := <-receivedMultiMTUCh:

			td := GetTDFromMulticast(mtu)
			//模拟接收剩下的切片
			d := randomDelay(mtu)
			log.Printf("收到sid: %v, blockID: %v, size: %v，模拟延时：%v.", td.SID, td.BlockID, td.SendSize, d)
			//记录fromIP:sid
			schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, td.FromIP)
			//处理TD
			schedule.GetCurPolicy().HandleTD(td)
			//内存回收
			config.TDBufferPool.Put(td.Buff)
		}
	}
}
func randomDelay(mtu config.MTU) time.Duration {
	var d time.Duration = 0
	d = time.Duration(mtu.FragmentCount - 1) * config.UDPDuration
	log.Printf("收到sid: %v, blockID: %v, size: %v，模拟延时：%v.", mtu.SID, mtu.BlockID, mtu.SendSize, d)
	time.Sleep(d)

	return d
}
func GetTDFromMulticast(message config.MTU) *config.TD  {
	//构造td
	td := &config.TD{
		SID:            message.SID,
		Buff:           common.ReadBlockWithSize(message.BlockID, message.SendSize),
		BlockID:        message.BlockID,
		MultiTargetIPs: message.MultiTargetIPs,
		FromIP:         message.FromIP,
		SendSize:       message.SendSize,
	}
	//td := config.TDBufferPool.Get().(*config.TD)
	//td.BlockID = message.BlockID
	//td.Buff = message.Data
	//td.FromIP = message.FromIP
	//td.MultiTargetIPs = message.MultiTargetIPs
	//td.SID = message.SID
	//td.SendSize = message.SendSize

	//log.Printf("GetTDFromMulticast：接收数据完成...td: sid: %v, sendSize: %v.\n", td.SID, td.SendSize)
	return td
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


func registerSafeExit()  {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			clearAll()
			os.Exit(0)
		}
	}()
}

func clearAll() {
	if curPolicy := schedule.GetCurPolicy(); curPolicy != nil {
		curPolicy.Clear()
	}

	//schedule.CloseAllChannels()
}

func finish() {
	done <- true
	clearAll()
}