package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"github.com/dchest/uniuri"
	"runtime"
	"testing"
	"time"
)

func TestMulticast(t *testing.T)  {

	//msgLog := map[string]config.MTU{} // key: "sid:fid"
	config.Init()
	randStr := uniuri.NewLen(100)
	buff := make([]byte, 100000)
	copy(buff, randStr)
	buff50 := buff[:10]
	fmt.Printf("%+v\n", buff50)



	schedule.SetPolicy(config.BASEMulticast)
	go common.ListenACK(schedule.MulticastReceiveAckCh)
	go common.Multicast(schedule.MulticastSendMTUCh)
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
			schedule.MulticastSendMTUCh <- *f
			select {
			case ack := <- schedule.MulticastReceiveAckCh:
				fmt.Printf("确认收到ack: %+v\n", ack)
			case <-time.After(2 * time.Millisecond):
				fmt.Printf("%v ack返回超时！\n", f.SID)
				schedule.MulticastSendMTUCh <- *f
			}
			//msgLog[common.StringConcat(strconv.Itoa(f.SID), ":", strconv.Itoa(f.FragmentID))] = *f
		}
	}
	for  {
		//select {
		//case ack := <-schedule.MulticastReceiveAckCh:
		//	fmt.Printf("确认收到ack: %+v\n", ack)
		//}
	}



	//schedule.GetCurPolicy().HandleCMD(cmd)
}

func testSliceMem(t *testing.T, f func([]int) []int) {
	t.Helper()
	ans := make([][]int, 0)
	for k := 0; k < 100; k++ {
		origin := make ([]int, 128 * 1024)
		ans = append(ans, f(origin))
	}
	printMem(t)
	_ = ans
}
func printMem(t *testing.T)  {
	t.Helper()
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	t.Logf("%.2f MB", float64(rtm.Alloc)/1024./1024.)
}

func Slice1(origin []int) []int {
	slice := make([]int, 0, cap(origin))
	copy(slice, origin)

	_ = origin
	return slice
}
func Slice2(origin []int) []int {
	slice := make([]int, 0, cap(origin))
	copy(slice, origin)

	return slice
}

func TestLastCharsBySlice1(t *testing.T)  { testSliceMem(t, Slice1)}
func TestLastCharsBySlice2(t *testing.T)  { testSliceMem(t, Slice2)}
