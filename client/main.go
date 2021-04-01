package main



import (
	common "EC/common"
	"EC/config"
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	readTrace("../example-traces/wdev_1.csv")
	go listenACK()
}
func listenACK() {

	listen, err := net.Listen("tcp", config.ClientIP + ":" +config.ClientACKListenPort)
	if err != nil {
		fmt.Printf("listen failed, err:%v", err)
		return
	}

	for {
		//等待客户端连接
		conn, err := listen.Accept()
		if err != nil {
			fmt.Printf("accept failed, err:%v", err)
			continue
		}
		//启动一个单独的goroutine去处理链接
		go handleACK(conn)
	}
}
func readTrace(fileName string) {

	fmt.Printf("read trace file: %s\n", fileName)
	/*******打开更新文件*********/
	fi, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()
	/*******读取更新请求*********/
	br := bufio.NewReader(fi)
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		str := strings.Split(string(a), ",")
		offset, _ := strconv.Atoi(str[4])   //更新偏移量
		readSize, _ := strconv.Atoi(str[5]) //更新数据大小
		//更新块的范围：（minBlockID，maxBlockID）
		minBlockID, maxBlockID := offset/config.ChunkSize, (offset+readSize)/config.ChunkSize
		/*******依次处理更新请求*******/
		for i := minBlockID; i <= maxBlockID; i++ {
			metaInfo := connectMS(i)
			//fmt.Printf("%v",metaInfo)
			updateData(metaInfo, config.ChunkSize)
		}
	}
}

func connectMS(chunkID int) config.MetaInfo {

	fmt.Printf("connect to ms : %s.\n", config.MSIP)
	/*******1.connect to MS, port : 8977********/
	request := &config.ReqData{
		OPType:  config.UpdateReq,
		ChunkID: chunkID,
	}
	res := common.SendData(request, config.MSIP, config.MSListenPort, "metaInfo")
	metaInfo, _ := res.(config.MetaInfo)

	return metaInfo
}

/*********inform datanode to update its local data***********/
func updateData(metaInfo config.MetaInfo, ChunkSize int) {

	fmt.Printf("inform datanode %d to update its local datachunk %d.\n",
												metaInfo.DataNodeID, metaInfo.DataChunkID)
	//generate random update data
	dataStr := common.RandStringBytesMask(ChunkSize)
	dataBytes := []byte(dataStr)
	td := &config.TD{
		OPType:      config.UpdateReq,
		Buff:        dataBytes,
		DataChunkID: metaInfo.DataChunkID,
	}
	//send data to datanode for update
	fmt.Printf("send datatype to datanode %d, IP address: %s\n", metaInfo.DataChunkID, metaInfo.ChunkIP)
	common.SendData(td, metaInfo.ChunkIP, config.NodeListenPort, "ack")

}
func handleACK(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var ack config.Ack
	err := dec.Decode(&ack)
	if err != nil {
		log.Fatal("client handleACK error: ", err)
	}
	fmt.Printf("client receiving ack: %d of updating chunk :%d\n",ack.AckID, ack.ChunkID)
}
