package main

import (
	"EC/common"
	"EC/config"
	"github.com/satori/go.uuid"
	//"strconv"
	"testing"
	"time"
)

func TestSend(t *testing.T) {
	sendCh := make(chan config.MTU, 10)
	receive := make(chan config.MTU, 10)
	go common.Multicast(sendCh)
	go common.ListenMulticast(receive)

	go RunPrintMsg(receive)
	time.Sleep(1 * time.Second)
	count := 0
	for{
		msg := &config.MTU{
			SID: count,
			BlockID: count,
			FromIP: common.GetLocalIP(),
			MultiTargetIPs: []string{common.GetLocalIP()},
			IsFragment: count % 2 == 0,
			FragmentCount: count,
			Data: []byte( uuid.NewV4().String()),
		}
		sendCh <- *msg
		time.Sleep(3 * time.Second)
		count += 1
	}
}
func RunPrintMsg(receiveCh <-chan config.MTU) {
	for{
		common.PrintMessage(<- receiveCh)
	}
}

