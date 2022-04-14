package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"bufio"
	"encoding/json"
	"github.com/wxnacy/wgo/arrays"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	//"strconv"
	"testing"
)
type Message struct {
	ID uint64 `json:"id"`
	Data []byte `json:"data"`
	MultiTargetIPs []string `json:"multiTargetIPs"`
}


func TestMulticast(t *testing.T) {

	policy := schedule.TUpdateB{}
	policy.Init()

	totalReqs := make([]*config.ReqData, 0, config.MaxBlockSize)
	OutFilePath := "../request/"+"hm_0"+"_"+"2.5E-01M.csv.txt"
	blockFile, err := os.Open(OutFilePath)
	defer blockFile.Close()
	//处理block请求
	if err != nil {
		log.Fatalln("Error: ", err)
	}

	bufferReader := bufio.NewReader(blockFile)
	for {
		lineData, _, err := bufferReader.ReadLine()
		if err == io.EOF {
			break
		}else if err != nil {
			log.Fatalln("handleReqFile error: ",err)
		}
		userRequestStr := strings.Split(string(lineData), ",")
		blockID, rangeLeft, rangeRight := 0, 0, config.BlockSize
		if len(userRequestStr) == 1 {
			blockID, _ = strconv.Atoi(userRequestStr[0])
		}else{
			blockID, _ = strconv.Atoi(userRequestStr[0])
			rangeLeft, _ = strconv.Atoi(userRequestStr[1])
			rangeRight, _ = strconv.Atoi(userRequestStr[2])
		}

		req := &config.ReqData{
			BlockID: blockID,
			RangeLeft: rangeLeft,
			RangeRight: rangeRight,
		}
		totalReqs = append(totalReqs, req)

	}

	schedule.BlockMerge(totalReqs)
		//sum := 0
		//
		//for i := 0; i < len(blockMap) - 1; i++ {
		//	sum += blockMap[i+1].RangeLeft - blockMap[i].RangeRight
		//}
		//if sum < 0 {
		//	fmt.Println( blockID, " 可以合并: ", sum)
		//}else {
		//	fmt.Println( blockID, " 建议不合并: ", sum)
		//}


	//对比2种方式：1）sync.pool  2）make slice 的内存占用情况



	//schedule.SetPolicy(config.BASEMulticast)
	//go common.ListenMulticast(schedule.MulticastReceiveMTUCh)
	//go common.HandlingACK(schedule.MulticastReceiveAckCh)
	////go MsgSorter(schedule.MulticastReceiveMTUCh, schedule.MulticastReceiveAckCh)
	//for  {
	//
	//}
}
func ListenMulticast(receive chan Message) {
	addr, err := net.ResolveUDPAddr("udp", config.MulticastAddrWithPort)
	common.PrintError("resolve error in ListenMulticast: ", err)
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	err = conn.SetReadBuffer(config.MaxDatagramSize)
	common.PrintError("set read buffer error in ListenMulticast: ", err)
	defer conn.Close()
	var message Message
	for  {
		inputBytes := make([]byte, config.MTUSize)
		length, _, err := conn.ReadFromUDP(inputBytes)
		common.PrintError("read UDP error in ListenMulticast: ", err)
		//buffer := bytes.NewBuffer(inputBytes[:length])
		err = json.Unmarshal(inputBytes[:length], &message)
		if err != nil {
			log.Printf("error decoding message response: %v", err)
			if e, ok := err.(*json.SyntaxError); ok {
				log.Printf("syntax error at byte offset %d", e.Offset)
			}
			//log.Printf("message response: %q", inputBytes[:length])
			//return err
		}
		//common.PrintError("unmarshal error in listenMulticast: ", err)

		//decoder := gob.NewDecoder(buffer)
		//_ = decoder.Decode(&message)
		if i := arrays.ContainsString(message.MultiTargetIPs, common.GetLocalIP()); i >= 0 {
			receive <- message
			log.Printf("received data : %v\n", message)
			//PrintMessage(message)
		}
	}
}

func TestReceive(t *testing.T) {
	config.InitBufferPool()
	receiveCh := make(chan config.MTU, 100)


	go common.ListenMulticast(receiveCh)

	for {
		select {
		case msg := <-receiveCh:
			//log.Printf("收到 sid %v: framentIndex:%v.\n", msg.SID, msg.FragmentID)
			//log.Printf("第一次接收sid: %v，总共需要%v个数据.", msg.SID, msg.FragmentCount)
			//td := GetTDFromMulticast(msg)
			//td.Buff = make([]byte, msg.SendSize)
			//模拟接收剩下的切片
			randomDelay(msg)
			//log.Printf(" %v剩余mtu，耗时：%v ms.", msg.SID, d)
			//time.Sleep(d)
			//log.Printf("sid %v相关数据接收完成！可以执行HandleTD了.", msg.SID)

		}

	}

}
func TestSend(t *testing.T) {

	config.Init()
	config.InitBufferPool()
	buff := common.ReadBlockWithSize(1, config.BlockSize)
	log.Println(len(buff))



	//for i := 0; i < 1; i++ {
	//	buff := common.ReadBlockWithSize(i+1, config.BlockSize)
	//	td := &config.TD{
	//		SID: i,
	//		BlockID: i+1,
	//		FromIP: common.GetLocalIP(),
	//		ToIP: "192.168.1.120",
	//		Buff: buff,
	//	}
	//	common.SendData(td, common.GetLocalIP(), config.NodeTDListenPort)
	//}
}
func TestListeningQuit(t *testing.T) {

	done := make(chan bool, 10)

	l, err := net.Listen("tcp", common.GetLocalIP() + ":" + config.NodeSettingsListenPort)
	if err != nil {
		log.Fatalln("listening ack err in listenAndReceive: ", err)
	}
	go listenQuit(l, done)

	//done <- true
	//done <- true
	//done <- true
	//done <- true
	//done <- true
	for  {
		//log.Printf("等待done信号...\n")
		select {
		case b := <- done:
			if b {
				log.Printf("真")
			}
			//log.Printf("结束!")
		default:
			log.Printf("超时！")
			return
		}

	}

}
func listenQuit(listen net.Listener, done chan<- bool) {
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
		p := common.GetPolicy(conn)
		log.Printf("Policy: %+v\n", p)
		if p.Type == -1 {
			done <- true
		}

		//config.AckBufferPool.Put(ack)
		//
		//connections = append(connections, conn)
		//if len(connections)%100 == 0 {
		//	log.Printf("total number of connections: %v", len(connections))
		//}
	}
}
func RunPrintMsg(receiveCh <-chan config.MTU) {
	for{
		common.PrintMessage(<- receiveCh)
	}
}


