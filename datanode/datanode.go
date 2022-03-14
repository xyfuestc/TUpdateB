package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"
)
//var connections []net.Conn

var done = make(chan bool)
var tickerDuration = time.Second * 4
var ticker = time.NewTicker(tickerDuration)

//func handleCMD(conn net.Conn)  {
//	defer conn.Close()
//	cmd := common.GetCMD(conn)
//	schedule.GetCurPolicy().RecordSIDAndReceiverIP(cmd.SID, common.GetConnIP(conn))
//	log.Printf("收到来自 %s 的命令: 将 sid: %d, block: %d 的更新数据发送给 %v.\n", common.GetConnIP(conn), cmd.SID, cmd.BlockID, cmd.ToIPs)
//	schedule.GetCurPolicy().HandleCMD(&cmd)
//}

//func handleACK(conn net.Conn) {
//	defer conn.Close()
//	ack := common.GetACK(conn)
//	schedule.GetCurPolicy().HandleACK(&ack)
//}
//
//func handleTD(conn net.Conn)  {
//	defer conn.Close()
//	td := common.GetTD(conn)
//	log.Printf("收到来自 %s 的TD，sid: %d, blockID: %d.\n", common.GetConnIP(conn), td.SID, td.BlockID)
//	schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, common.GetConnIP(conn))
//	schedule.GetCurPolicy().HandleTD(&td)
//}

func setPolicy(conn net.Conn)  {
	defer conn.Close()
	p := common.GetPolicy(conn)

	//检测结束
	if p.Type == -1 {
		//finish()
		return

	}else if strings.Contains(config.Policies[p.Type], "Multicast") {
		//启动超时处理
		ticker.Reset(tickerDuration)

	}else{
		ticker.Stop()
	}

	schedule.SetPolicy(config.Policies[p.Type])
	config.BlockSize = int(p.NumOfMB * config.Megabyte)
	config.RSBlockSize = int(p.NumOfMB * config.Megabyte) * config.W

	log.Printf("初始化共享池...\n")
	config.InitBufferPool()

	log.Printf("收到来自 %s 的命令，设置当前算法设置为%s, 当前blockSize=%.2fMB，RSBlockSize=%dMB.\n",
		common.GetConnIP(conn), config.Policies[p.Type], p.NumOfMB, config.RSBlockSize/config.Megabyte)

}

func finish() {
	done <- true
	ticker.Stop()
	clearAll()

}
func msgSorter(receivedAckCh <-chan config.ACK, receivedTDCh <-chan config.TD, receivedCMDCh <-chan config.CMD)  {
	for  {

		select {
		case ack := <-receivedAckCh:
			schedule.GetCurPolicy().HandleACK(&ack)

		case td := <-receivedTDCh:
			schedule.GetCurPolicy().HandleTD(&td)

		case cmd := <-receivedCMDCh:
			schedule.GetCurPolicy().HandleCMD(&cmd)

		case <- ticker.C:
			log.Printf("处理超时！")
			schedule.HandleTimeout()
			//if count == 0 {
			//	ticker.Stop()
			//}
		}
	}
}
func main() {
	//defer profile.Start(profile.MemProfile, profile.MemProfileRate(1)).Stop()

	//先暂停处理超时
	ticker.Stop()

	config.Init()

	//监听并接收ack，检测程序结束
	listenAndReceive(config.NumOfWorkers)

	//当发生意外退出时，安全释放所有资源
	registerSafeExit()


	////清除连接
	//defer func() {
	//	for _, conn := range connections {
	//		conn.Close()
	//	}
	//}()

	for  {
		select {
		case <- done:
			fmt.Printf("收到结束信号...退出\n")
			return
		}
	}


	//go listenTD(l3)
	//go listenACK(l2)
	//go listenSettings(l4)
	//go timeout()
	//go common.ListenACK(schedule.MulticastReceiveAckCh)

	//listenCMD(l1)
}

func listenAndReceive(maxWorkers int) {
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

	go listenCMD(l1)
	go listenACK(l2)
	go listenTD(l3)
	go listenSettings(l4)
	go common.Multicast(schedule.MulticastSendMTUCh)

	for i := 0; i < maxWorkers; i++ {
		go msgSorter(schedule.ReceivedAckCh, schedule.ReceivedTDCh, schedule.ReceivedCMDCh)
	}
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
		cmd := common.GetCMD(conn)
		log.Printf("收到来自 %s 的命令: 将 sid: %d, block: %d 的更新数据发送给 %v.\n", common.GetConnIP(conn), cmd.SID, cmd.BlockID, cmd.ToIPs)

		schedule.GetCurPolicy().RecordSIDAndReceiverIP(cmd.SID, common.GetConnIP(conn))
		schedule.ReceivedCMDCh <- cmd
		//config.CMDBufferPool.Put(cmd)


		//connections = append(connections, conn)
		//if len(connections)%100 == 0 {
		//	log.Printf("total number of connections: %v", len(connections))
		//}
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
		ack := common.GetACK(conn)
		//log.Printf("收到来自 %v 的 ack： %+v\n", common.GetConnIP(conn), ack)
		schedule.ReceivedAckCh <- ack
		//config.AckBufferPool.Put(ack)

		//connections = append(connections, conn)
		//if len(connections)%100 == 0 {
		//	log.Printf("total number of connections: %v", len(connections))
		//}
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
		schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, common.GetConnIP(conn))
		schedule.ReceivedTDCh <- td
		//config.TDBufferPool.Put(td)

		log.Printf("收到来自 %s 的TD，sid: %d, blockID: %d.\n", common.GetConnIP(conn), td.SID, td.BlockID)

		//connections = append(connections, conn)
		//if len(connections)%100 == 0 {
		//	log.Printf("total number of connections: %v", len(connections))
		//}
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
		setPolicy(conn)

		//connections = append(connections, conn)
	}
}
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
	//for _, conn := range connections {
	//	conn.Close()
	//}
	if curPolicy := schedule.GetCurPolicy(); curPolicy != nil {
		curPolicy.Clear()
	}
	//schedule.CloseAllChannels()

}



