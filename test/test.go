package test

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/satori/go.uuid"
	//"strconv"
	"testing"
	"time"
)

func TestSend(t *testing.T) {
	sendCh := make(chan config.MTU)
	receive := make(chan config.MTU)
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
		PrintMessage(<- receiveCh)
	}
}
func PrintMessage(data config.MTU) {
	fmt.Println("=== Data received ===")
	fmt.Println("ID: ", data.SID)
	fmt.Println("BlockID: ", data.BlockID)
	fmt.Println("FromIP:", data.FromIP)
	fmt.Println("MultiTargetIPs:", data.MultiTargetIPs)
	fmt.Println("FragmentCount:", data.FragmentCount)
	fmt.Println("IsFragment:", data.IsFragment)
	fmt.Println("= Data =")
	fmt.Println("Content:", data.Data)
}
