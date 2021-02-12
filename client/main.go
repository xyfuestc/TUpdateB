package main

import (
	"EC/common"
	"EC/config"
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	readTrace("./example-traces/wdev_1.csv")
	//readTrace("./example-traces/test.csv")
}

func readTrace(fileName string) {
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
	/*******1.与服务器建立连接，端口：8977********/
	request := &config.UpdateReqData{
		OPType:       config.UPDT_REQ,
		LocalChunkID: chunkID,
	}
	res := common.SendData(request, config.HOST_IP, config.ListenPort, "metaInfo")
	metaInfo, _ := res.(config.MetaInfo)

	return metaInfo
}

/*********通知DataNode，更新数据块***********/
func updateData(metaInfo config.MetaInfo, ChunkSize int) {

	dataStr := common.RandStringBytesMask(ChunkSize)
	dataBytes := []byte(dataStr)
	td := &config.TD{
		OPType:      config.UPDT_REQ,
		Buff:        dataBytes,
		DataChunkID: metaInfo.DataChunkID,
	}
	res := common.SendData(td, metaInfo.ChunkIP, config.NodeListenClientPort, "ack")
	ack, ok := res.(config.ACKData)
	if ok {
		fmt.Printf("成功更新数据块：%d\n", ack.ChunkID)
	} else {
		log.Fatal("client updateData 解码出错!")
	}

}
