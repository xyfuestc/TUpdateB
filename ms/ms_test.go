package ms

import (
	"EC/client"
	"EC/common"
	"EC/config"
	"EC/schedule"
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	//"strconv"
	"testing"
)
type Message struct {
	ID uint64 `json:"id"`
	Data []byte `json:"data"`
	MultiTargetIPs []string `json:"multiTargetIPs"`
}
const TIME_LAYOUT = "2006-01-02 15:04:05"

func parseWithLocation(name string, timeStr string) (time.Time, error) {
	locationName := name
	if l, err := time.LoadLocation(locationName); err != nil {
		println(err.Error())
		return time.Time{}, err
	} else {
		lt, _ := time.ParseInLocation(TIME_LAYOUT, timeStr, l)
		fmt.Println(locationName, lt)
		return lt, nil
	}
}
func recordSpaceAndTime(space int, spendTime time.Duration, averageSpace float64)  {
	var blockFile *os.File
	if client.CheckFileIsExist(SpaceFilePath) { //如果文件存在
		blockFile, _ = os.OpenFile(SpaceFilePath, os.O_APPEND|os.O_WRONLY, 0666)
		fmt.Println("文件存在")
	} else {
		blockFile, _ = os.Create(SpaceFilePath) //创建文件
		fmt.Println("文件不存在")
	}
	write := bufio.NewWriter(blockFile)

	strTime := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
	averageSpaceStr := fmt.Sprintf("%f", averageSpace)
	var str = strTime + " : " + strconv.Itoa(space) + ", " + averageSpaceStr + ", " + spendTime.String()  + "\n"
	write.WriteString(str)
	write.Flush()
	defer blockFile.Close()
}

func TestSpace(t *testing.T)  {
	config.Init()

	//监听ack
	log.Printf("ms启动...")
	log.Printf("监听ack: %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)

	//当发生意外退出时，释放所有资源
	registerSafeExit()
	//监听并接收ack，检测程序结束
	listenAndReceive(config.NumOfWorkers)

	curPolicy = 3
	//GetReqsFromTrace()
	//curPolicyVal := atomic.LoadInt32(&curPolicy)
	//traceName = "hm_0_2.5E-0"
	totalReqs = GetReqsFromTrace()
	//space := 0.0
	averageSpace := 0.0
	for averageSpace != -1 {

		fmt.Println("当前步长：", averageSpace)
		schedule.AverageSpace = averageSpace
		start(totalReqs)
		//保证主线程运行
		for  {
			isRoundFinished := atomic.LoadInt32(&roundFinished)
			if isRoundFinished == 1 {
				recordSpaceAndTime(0, sumTime, averageSpace)
				//进入下一轮
				//atomic.AddInt32(&curPolicy, 1)
				break
			}
		}
		_, averageSpace = schedule.BlockMergeWithAverageSpace(totalReqs, averageSpace)
		//curPolicyVal = atomic.LoadInt32(&curPolicy)
		//curPolicy++

	}
	//清空
	clearAll()

	//totalReqs = GetReqsFromTrace()
	//space := 0.0
	//for space != -1 {
	//	fmt.Printf("%+v\n", space)
	//	_,nextSpace,_ := schedule.BlockMergeWithAverageSpace(totalReqs, space)
	//	//for _, req := range mergeReqs{
	//	//	fmt.Printf("%+v\n", req)
	//	//}
	//	space = nextSpace
	//}
}

func TestAverageSpace(t *testing.T) {

	config.Init()

	//监听ack
	log.Printf("ms启动...")
	log.Printf("监听ack: %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)

	//当发生意外退出时，释放所有资源
	registerSafeExit()
	//监听并接收ack，检测程序结束
	listenAndReceive(config.NumOfWorkers)

	curPolicy = 3
	//GetReqsFromTrace()
	//curPolicyVal := atomic.LoadInt32(&curPolicy)
	//traceName = "hm_0_2.5E-0"
	totalReqs = GetReqsFromTrace()
	space := 0
	averageSpaceIncrement := 0.0
	for space != -1 {

		fmt.Println("当前步长：", space)
		schedule.Space = space
		start(totalReqs)
		//保证主线程运行
		for  {
			//isRoundFinished := atomic.LoadInt32(&roundFinished)

			select {
			case <-ScheduleFinishedChan:
				recordSpaceAndTime(space, sumTime, averageSpaceIncrement)
				//进入下一轮
				//atomic.AddInt32(&curPolicy, 1)
				break
			}
		}
		_, space = schedule.BlockMergeWithSpace(totalReqs, space)
		//curPolicyVal = atomic.LoadInt32(&curPolicy)
		//curPolicy++

	}
	//清空
	clearAll()
	//通知各个节点退出
	//notifyNodesQuit()

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


