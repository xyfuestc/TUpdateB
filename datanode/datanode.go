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

//处理client更新请求
func handleUpdateReq(conn net.Conn) {

	defer conn.Close()
	//解析请求
	dec := gob.NewDecoder(conn)

	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleUpdateReq:datanode更新数据，解码出错: ", err)
	}

	switch td.OPType {
	case config.UPDT_REQ:
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

/***********处理MS的命令***********/
/*
* 1.针对Data-Delta Update，接收MS命令，将数据直接发给对应的rootParity
* 2.针对Parity-Delta Update，接收MS命令，1）若为leafNode，将数据转发给root 2）若为rootNode，将数据分发给对应parity
 */
func handleMSCMD(conn net.Conn) {
	defer conn.Close()
	//解析请求
	dec := gob.NewDecoder(conn)

	var cmd config.CMD
	err := dec.Decode(&cmd)
	if err != nil {
		log.Fatal("handleMSCMD: 解码出错: ", err)
	}
	index := cmd.DataChunkID
	toIP := cmd.ToIP

	switch cmd.Type {
	//若为DDU模式，则只需直接将数据发给rootParity
	case config.DataDeltaUpdate:
		//读取DataChunkID对应数据
		var buff = make([]byte, config.ChunkSize, config.ChunkSize)
		file, err := os.OpenFile("./data/dataFile", os.O_RDONLY, 0)

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

		//发送读取的数据给对应的Parity
		td := config.TD{
			OPType:      config.SendDataToParity,
			DataChunkID: cmd.DataChunkID,
			Buff:        buff,
		}

		res := common.SendData(td, toIP, config.DataPort, "ack")
		ack, ok := res.(config.ACKData)
		if ok {
			fmt.Printf("成功更新数据块：%d\n", ack.ChunkID)
			common.SendData(ack, config.MSIP, config.ACKPort, "")
		} else {
			log.Fatal("client updateData 解码出错!")
		}

	}

}
func main() {
	listenClientReq()
	listenMSCMD()
}

func listenMSCMD() {
	listenAddr := common.GetLocalIP()
	listenAddr = listenAddr + ":" + strconv.Itoa(config.NodeListenMSCMDPort)
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
		go handleMSCMD(conn)
	}
}

func listenClientReq() {
	listenAddr := common.GetLocalIP()
	listenAddr = listenAddr + ":" + strconv.Itoa(config.NodeListenClientPort)
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
		go handleUpdateReq(conn)

	}
}
