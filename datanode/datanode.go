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

//handle req
func handleReq(conn net.Conn) {

	defer conn.Close()
	//decode the req
	dec := gob.NewDecoder(conn)

	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
	}


	switch td.OPType {
	//data update from client
	case config.UpdateReq:
		buff := td.Buff
		file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)
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

		ack := &config.ReqData{
			ChunkID: td.DataChunkID,
			AckID: td.DataChunkID+1,    //ackID=chunkID+1
		}

		enc := gob.NewEncoder(conn)
		err = enc.Encode(ack)
		if err != nil {
			fmt.Printf("encode err:%v", err)
			return
		}

	//DDU mode, send data to root parity
	case config.DDU:

		cmd := td
		index := cmd.DataChunkID
		//read data from disk
		var buff = make([]byte, config.ChunkSize, config.ChunkSize)
		file, err := os.OpenFile(config.DataFilePath, os.O_RDONLY, 0)

		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()
		readSize, err := file.ReadAt(buff, int64((index-1)*config.ChunkSize))

		if err != nil {
			log.Fatal("读取文件失败：", err)
		}
		if readSize != config.ChunkSize {
			log.Fatal("读取数据块失败！读取大小为：", readSize)
		}

		//send data to root parity
		sendData := config.TD{
			OPType:      config.DDURoot,
			DataChunkID: cmd.DataChunkID,
			Buff:        buff,
		}
		//get ack to ms
		res := common.SendData(sendData, cmd.ToIP, config.NodeListenPort, "ack")
		ack, ok := res.(config.ReqData)
		if ok {
			fmt.Printf("成功更新数据块：%d\n", ack.ChunkID)
			common.SendData(ack, config.MSIP, config.MSListenPort, "")
		} else {
			log.Fatal("client updateData 解码出错!")
		}

	}


}

func main() {
	listenReq()
}

func listenReq() {
	//listen Req
	listenAddr := common.GetLocalIP()
	listenAddr = listenAddr + ":" + strconv.Itoa(config.NodeListenPort)
	fmt.Printf("client req listening: %s",listenAddr)
	listenReq, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Printf("listenReq failed, err:%v\n", err)
		return
	}

	for {
		//等待客户端连接
		conn, err := listenReq.Accept()
		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		//启动一个单独的goroutine去处理链接
		go handleReq(conn)

	}
}
