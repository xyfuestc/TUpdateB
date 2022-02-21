package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"testing"
)

func TestMulticast(t *testing.T)  {

	//msgLog := map[string]config.MTU{} // key: "sid:fid"

	schedule.SetPolicy(config.BASEMulticast)
	//go common.ListenACK(schedule.ReceiveAck)
	go common.Multicast(schedule.SendCh)
	for i := 0; i < 10; i++ {
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
			//msgLog[common.StringConcat(strconv.Itoa(f.SID), ":", strconv.Itoa(f.FragmentID))] = *f
		}
	}




	//schedule.GetCurPolicy().HandleCMD(cmd)
}
