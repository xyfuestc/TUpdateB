package common

import (
	"EC/config"
	"encoding/json"
	"github.com/mailru/easyjson"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"net"
)

type Packet struct {
	SourceIP, DestinationIP, ID, Response string
	Content                               []byte
}
func Run(sendCh chan config.MTU) <-chan config.MTU  {
	receive := make(chan config.MTU)
	go ListenMulticast(receive)
	go Multicast(sendCh)
	//go msgSorter(receive)
	return receive
}
/*发送数据到多播地址*/
func Multicast(send chan config.MTU) {
	addr, err := net.ResolveUDPAddr("udp", config.MulticastAddrWithPort)
	PrintError("ResolvingUDPAddr in Multicast failed: ", err)
	conn, err := net.DialUDP("udp", nil, addr)
	PrintError("DialUDP error in Multicast: ", err)
	//var buffer bytes.Buffer
	//encoder := gob.NewEncoder(&buffer)
	for  {
		message := <-send
		//msg, err := json.Marshal(message)
		msg, err := easyjson.Marshal(message)
		//err := encoder.Encode(message)
		PrintError("Encode error in Multicast: ", err)
		_, err = conn.Write(msg)
		PrintError("conn write error in Multicast: ", err)
		//log.Printf("发送sid: %v的第%v（共%d）个分片数据.", message.SID, message.FragmentID, message.FragmentCount)
	}
}

/*发送数据到原地址*/
func HandlingACK(ackCh chan config.ACK) {
	for  {
		ack := <-ackCh
		receiverAddr := StringConcat(GetNodeIP(GetNodeID(ack.BlockID)), "", config.MulticastAddrListenACK)
		//receiverAddr := StringConcat("localhost", "", config.MulticastAddrListenACK)
		addr, err := net.ResolveUDPAddr("udp", receiverAddr)
		PrintError("ResolvingUDPAddr in Multicast failed: ", err)
		conn, err := net.DialUDP("udp", nil, addr)
		PrintError("DialUDP error in Multicast: ", err)
		//msg, err := json.Marshal(ack)
		msg, err := easyjson.Marshal(ack)
		//err := encoder.Encode(ack)
		PrintError("Encode error in Multicast: ", err)
		_, err = conn.Write(msg)
		PrintError("conn write error in Multicast: ", err)
		log.Printf("向 %v 发送 ack: %+v\n.", GetLocalIP(), ack)
	}
}

func ListenACK(receiveACKCh chan config.ACK)  {
	addr, err := net.ResolveUDPAddr("udp", StringConcat("localhost","", config.MulticastAddrListenACK))
	PrintError("resolve error in ListenMulticast: ", err)
	log.Printf("listening multicast ack at %v...\n", addr)
	conn, err := net.ListenUDP("udp", addr)
	//err = conn.SetReadBuffer(config.MaxDatagramSize)
	PrintError("set read buffer error in ListenMulticast: ", err)
	defer conn.Close()
	var ack config.ACK
	for  {
		inputBytes := make([]byte, config.MaxDatagramSize)
		length, _, err := conn.ReadFromUDP(inputBytes)
		PrintError("read UDP error in ListenMulticast: ", err)
		err = easyjson.Unmarshal(inputBytes[:length], &ack)
		if err != nil {
			log.Printf("error decoding ack response: %v", err)
			if e, ok := err.(*json.SyntaxError); ok {
				log.Printf("syntax error at byte offset %d", e.Offset)
			}
			log.Printf("ack response: %q", inputBytes[:length])
			//return err
		}
		receiveACKCh <- ack

	}
}

/*监听多播地址的数据，如果是发送给自己的，就接收*/
func ListenMulticast(receive chan config.MTU) {
	addr, err := net.ResolveUDPAddr("udp", config.MulticastAddrWithPort)
	PrintError("resolve error in ListenMulticast: ", err)
	log.Printf("listening multicast at %v...\n", addr)
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	err = conn.SetReadBuffer(config.MaxDatagramSize)
	PrintError("set read buffer error in ListenMulticast: ", err)
	defer conn.Close()
	var message config.MTU
	for  {
		inputBytes := make([]byte, config.MaxDatagramSize)
		length, _, err := conn.ReadFromUDP(inputBytes)
		PrintError("read UDP error in ListenMulticast: ", err)
		err = easyjson.Unmarshal(inputBytes[:length], &message)
		if err != nil {
			log.Printf("error decoding message response: %v", err)
			if e, ok := err.(*json.SyntaxError); ok {
				log.Printf("syntax error at byte offset %d", e.Offset)
			}
			log.Printf("message response: %q", inputBytes[:length])
			//return err
		}
		if i := arrays.ContainsString(message.MultiTargetIPs, GetLocalIP()); i >= 0 {
			receive <- message
		}
	}
}
