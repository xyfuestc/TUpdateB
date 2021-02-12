package main

import (
	"EC/common"
	"EC/config"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

func main() {
	listenData()
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	//解析请求
	dec := gob.NewDecoder(conn)

	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleUpdateReq:parityNode更新数据，解码出错: ", err)
	}

	switch td.OPType {
	//接收dataNode数据
	case config.SendDataToParity:
		//接收来的数据
		buff := td.Buff

		oldBuff := make([]byte, config.ChunkSize, config.ChunkSize)

		file, err := os.OpenFile("./data/dataFile", os.O_RDWR, 0)
		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()
		index := td.StripeID
		file.ReadAt(oldBuff, int64(index*config.ChunkSize))

		//进行数据融合（异或）
		for i := 0; i < len(buff); i++ {
			buff[i] = buff[i] ^ oldBuff[i]
		}
		//写入文件
		file.Write(buff)

		//返回ack
		ack := &config.ACKData{
			ChunkID: td.DataChunkID,
		}
		enc := gob.NewEncoder(conn)
		err = enc.Encode(ack)
		if err != nil {
			fmt.Printf("parityNode : handleRequest encode err:%v", err)
			return
		}
	//接收其他DataNode的更新数据
	case config.MoveDataToRoot:
		buff := td.Buff
		file, err := os.OpenFile("./data/dataFile", os.O_RDWR, 0)
		//1.打开文件后，光标默认在文件开头。
		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()
		index := td.StripeID
		file.Seek(int64((index-1)*config.ChunkSize), 0)
		file.Write(buff)
		//fmt.Printf("更新datanode成功！更新大小：%d B\n", config.ChunkSize)

		ack := &config.ACKData{
			ChunkID: td.DataChunkID,
		}

		enc := gob.NewEncoder(conn)
		err = enc.Encode(ack)
		if err != nil {
			fmt.Printf("encode err:%v", err)
			return
		}
	}

}
func listenData() {
	listenAddr := common.GetLocalIP()
	listenAddr = listenAddr + ":" + strconv.Itoa(config.DataPort)
	fmt.Println(listenAddr)
	listen, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Printf("listen failed, err:%v", err)
		return
	}
	for {
		//等待客户端连接
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		//启动一个单独的goroutine去处理链接
		go handleRequest(conn)
	}
}
