package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"testing"
	"time"
)

func TestMulticast(t *testing.T)  {

	//msgLog := map[string]config.MTU{} // key: "sid:fid"

	schedule.SetPolicy(config.BASEMulticast)
	go common.ListenACK(schedule.ReceiveAck)
	go common.Multicast(schedule.SendCh)
	for i := 0; i < 100; i++ {
		cmd := &config.CMD{
			SID:      i,
			BlockID:  i,
			FromIP:   common.GetLocalIP(),
			ToIPs:    []string{common.GetLocalIP()},
			SendSize: 1024,
		}
		fragments := schedule.GetFragments(cmd)
		for _, f := range fragments {
			schedule.SendCh <- *f
			select {
			case ack := <- schedule.ReceiveAck:
				fmt.Printf("确认收到ack: %+v\n", ack)
			case <-time.After(2 * time.Millisecond):
				fmt.Printf("%v ack返回超时！\n", f.SID)
				schedule.SendCh <- *f
			}
			//msgLog[common.StringConcat(strconv.Itoa(f.SID), ":", strconv.Itoa(f.FragmentID))] = *f
		}
	}
	for  {
		//select {
		//case ack := <-schedule.ReceiveAck:
		//	fmt.Printf("确认收到ack: %+v\n", ack)
		//}
	}



	//schedule.GetCurPolicy().HandleCMD(cmd)
}
