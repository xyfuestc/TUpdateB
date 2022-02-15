package common

import (
	"EC/config"
	"bytes"
	"encoding/gob"
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
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	for  {
		message := <-send
		err := encoder.Encode(message)
		PrintError("Encode error in Multicast: ", err)
		_, err = conn.Write(buffer.Bytes())
		log.Printf("发送数据: %v\n", message)
		PrintError("conn write error in Multicast: ", err)
		buffer.Reset()
	}
}

/*监听多播地址的数据，如果是发送给自己的，就接收*/
func ListenMulticast(receive chan config.MTU) {
	addr, err := net.ResolveUDPAddr("udp", config.MulticastAddrWithPort)
	PrintError("resolve error in ListenMulticast: ", err)
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	err = conn.SetReadBuffer(config.MaxDatagramSize)
	PrintError("set read buffer error in ListenMulticast: ", err)
	defer conn.Close()
	var message config.MTU
	for  {
		inputBytes := make([]byte, config.MTUSize)
		length, _, err := conn.ReadFromUDP(inputBytes)
		PrintError("read UDP error in ListenMulticast: ", err)
		buffer := bytes.NewBuffer(inputBytes[:length])
		decoder := gob.NewDecoder(buffer)
		_ = decoder.Decode(&message)
		if i := arrays.ContainsString(message.MultiTargetIPs, GetLocalIP()); i >= 0 {
			receive <- message
		}
	}
}

