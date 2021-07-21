package datanode

import (
	"EC/common"
	"EC/config"
	"EC/ms"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"time"
)

//handle req
//func handleReq(conn net.Conn) {
//	//targetIP := strings.Split(conn.RemoteAddr().String(),":")[0]
//
//	defer conn.Close()
//	//decode the req
//	dec := gob.NewDecoder(conn)
//	var td config.TD
//	err := dec.Decode(&td)
//	if err != nil {
//		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
//	}
//
//	switch td.OPType {
//	case config.UpdateReq:
//		//cau.HandleReq(td)
//	}
//}
var SidCounter = 0
var WaitingACKGroup = make(map[int]config.WaitingACKItem)

func handleCMD(conn net.Conn)  {
	cmd := common.GetCMD(conn)
	ms.GetCurPolicy().HandleCMD(cmd)
}

func handleACK(conn net.Conn) {
	ack := common.GetACK(conn)
	ms.GetCurPolicy().HandleACK(ack)
}
func main() {
	config.Init()

	fmt.Printf("listening req in %s:%s\n",common.GetLocalIP(), config.NodeListenPort)
	l1, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeListenPort)
	if err != nil {
		fmt.Printf("listenReq failed, err:%v\n", err)
		return
	}

	fmt.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)
	l2, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeCMDListenPort)
	if err != nil {
		fmt.Printf("listenCMD failed, err:%v\n", err)
		return
	}

	fmt.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	l3, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeACKListenPort)
	if err != nil {
		fmt.Printf("listenACK failed, err:%v\n", err)
		return
	}

	go listenCMD(l2)
	go listenACK(l3)
	listenReq(l1)

}
func handleTestSpeed(conn net.Conn){
	defer conn.Close()
	//decode the req
	dec := gob.NewDecoder(conn)
	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
	}
	time.Now().Second()

	fmt.Printf("%s : receiving data: %v\n",time.Now().Format("2006-01-02 15:04:04"), td.BlockID)

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
		handleTestSpeed(conn)


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




