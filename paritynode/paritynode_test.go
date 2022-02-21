package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"encoding/json"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"net"

	//"strconv"
	"testing"
)
type Message struct {
	ID uint64 `json:"id"`
	Data []byte `json:"data"`
	MultiTargetIPs []string `json:"multiTargetIPs"`
}

func TestMulticast(t *testing.T) {
	schedule.SetPolicy(config.BASEMulticast)
	go common.ListenMulticast(schedule.ReceiveCh)
	go common.HandlingACK(schedule.ReceiveAck)
	go MsgSorter(schedule.ReceiveCh, schedule.ReceiveAck)
	for  {
		
	}
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
			log.Printf("message response: %q", inputBytes[:length])
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
	receiveCh := make(chan config.MTU, 100)
	go common.ListenMulticast(receiveCh)

	for {
		select {
		case msg := <-receiveCh:
			common.PrintMessage(msg)
		}
	}
}
func RunPrintMsg(receiveCh <-chan config.MTU) {
	for{
		common.PrintMessage(<- receiveCh)
	}
}

