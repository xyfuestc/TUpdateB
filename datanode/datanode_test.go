package main

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"github.com/dchest/uniuri"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
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

func testSliceMem(t *testing.T, f func(b, size int) ) {
	t.Helper()
	//ans := make([][]byte, 0)
	config.InitBufferPool()
	for k := 0; k < 100; k++ {
		go f(k, config.RSBlockSize)
	}
	time.Sleep(2 * time.Second)
	printMem(t)
	//_ = ans
}
func printMem(t *testing.T)  {
	t.Helper()
	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	t.Logf("%.2f MB", float64(rtm.Alloc)/1024./1024.)
}

func readBySyncPool(b, size int)  {
	index := common.GetIndex(b)
	//read data from disk
	//buff := make([]byte, size)

	buff := config.BlockBufferPool.Get().([]byte)

	file, err := os.OpenFile(config.DataFilePath, os.O_RDONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}

	defer file.Close()
	//fmt.Println(len(buff))
	readSize, err := file.ReadAt(buff[:size], int64(index * size))
	//fmt.Println(len(buff))
	if err != nil {
		log.Fatal("读取文件失败：", err)
	}
	if readSize != size {
		log.Printf("读取大小为不一致 in ReadBlockWithSize：%+v, %+v", readSize, size)
	}

}

func readBySlice(b, size int) {
	index := common.GetIndex(b)
	//read data from disk
	buff := make([]byte, size)
	//buff := config.BlockBufferPool.Get().([]byte)

	file, err := os.OpenFile(config.DataFilePath, os.O_RDONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}

	defer file.Close()

	readSize, err := file.ReadAt(buff[:size], int64(index * size))

	if err != nil {
		log.Fatal("读取文件失败：", err)
	}
	if readSize != size {
		log.Printf("读取大小为不一致 in ReadBlockWithSize：%+v, %+v", readSize, size)
	}

	//return buff[:size]
}

func TestMemBySyncPool(t *testing.T)  { testSliceMem(t, readBySyncPool)}
func TestMemBySlice(t *testing.T)  { testSliceMem(t, readBySlice)}



/*对比字符串拼接性能*/
func builderConcat(n int, str string) string {
	var builder strings.Builder
	for i := 0; i < n; i++ {
		builder.WriteString(str)
	}
	return builder.String()
}
func plusConcat(n int, str string) string {
	for i := 0; i < n; i++ {
		str = str + strconv.Itoa(i)
	}
	return str
}

func benchmark(b *testing.B, f func(int, string) string) {
	var str = uniuri.NewLen(10)
	for i := 0; i < b.N; i++ {
		f(10000, str)
	}
}

func BenchmarkPlusConcat(b *testing.B)    { benchmark(b, plusConcat) }
func BenchmarkBuilderConcat(b *testing.B)    { benchmark(b, builderConcat) }


func TestListenTD(t *testing.T) {
	log.Printf("listening td in %s:%s\n", common.GetLocalIP(), config.NodeTDListenPort)
	listen, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeTDListenPort)
	if err != nil {
		log.Printf("listening ack failed, err:%v\n", err)
		return
	}
	defer listen.Close()
	for {
		//等待客户端连接
		conn, e := listen.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}
		td := common.GetTD(conn)
		//schedule.GetCurPolicy().RecordSIDAndReceiverIP(td.SID, common.GetConnIP(conn))
		//schedule.ReceivedTDCh <- td
		//config.TDBufferPool.Put(td)

		log.Printf("收到来自 %s 的TD，sid: %d, blockID: %d.\n", common.GetConnIP(conn), td.SID, td.BlockID)
		err := conn.Close()
		if err != nil {
			log.Fatalln("error : ", err)
		}

		//connections = append(connections, conn)
		//if len(connections)%100 == 0 {
		//	log.Printf("total number of connections: %v", len(connections))
		//}
	}
}