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
	listen()
}

func handleReq(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleUpdateReq:parityNode更新数据，解码出错: ", err)
	}

	switch td.OPType {
	/*
		send data to root (in the same rack),
		in DDU mode：1）as root, receive data from datanode
	*/
	case config.DDURoot:
		//received data
		buff := td.Buff

		oldBuff := make([]byte, config.ChunkSize, config.ChunkSize)

		file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)
		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()
		index := td.StripeID
		file.ReadAt(oldBuff, int64(index*config.ChunkSize))

		//cau compute parity new value
		row := 0
		col := td.DataChunkID - (td.DataChunkID/config.K)*config.K
		factor := config.RS.GenMatrix[row*config.K+col]
		for i := 0; i < len(buff); i++ {
			buff[i] =  config.Gfmul(factor, buff[i]) ^ oldBuff[i]
		}
		//write to file
		file.Write(buff)

		acks := 0 //count ack

		for _, leafIP := range td.NextIPs {

			//send data to leaf
			data := &config.TD{
				OPType:      config.DDULeaf,
				Buff:        buff,
				DataChunkID: td.DataChunkID,
				ToIP: leafIP,
			}
			//send update data to leaf, wait for ack
			common.SendData(data, leafIP, config.NodeListenPort, "ack")

		}
		//return stripe ack (stripe update finished)
		if acks == len(td.NextIPs){
			//return ack
			ack := &config.ReqData{
				ChunkID: td.DataChunkID,
				AckID: td.DataChunkID+1,
			}
			enc := gob.NewEncoder(conn)
			err = enc.Encode(ack)
			if err != nil {
				fmt.Printf("parityNode : handleReq encode err:%v", err)
				return
			}
		}
	//2) as leaf, receive data from root
	case config.DDULeaf:
		//received data
		buff := td.Buff

		oldBuff := make([]byte, config.ChunkSize, config.ChunkSize)

		file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)
		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()
		index := td.StripeID
		file.ReadAt(oldBuff, int64(index*config.ChunkSize))


		//cau compute parity new value
		row := common.GetParityIDFromIP(td.ToIP)
		col := td.DataChunkID - (td.DataChunkID/config.K)*config.K
		factor := config.RS.GenMatrix[row*config.K+col]
		for i := 0; i < len(buff); i++ {
			buff[i] =  config.Gfmul(factor, buff[i]) ^ oldBuff[i]
		}
		//write to file
		file.Write(buff)

		//return ack
		ack := &config.ReqData{
			ChunkID: td.DataChunkID,
			AckID: td.DataChunkID+1,
		}
		enc := gob.NewEncoder(conn)
		err = enc.Encode(ack)
		if err != nil {
			fmt.Printf("parityNode : handleReq encode err:%v", err)
			return
		}

	case config.PDU:
		buff := td.Buff
		oldBuff := make([]byte, config.ChunkSize, config.ChunkSize)
		file, err := os.OpenFile(config.DataFilePath, os.O_RDWR, 0)

		if err != nil {
			fmt.Printf("打开文件出错：%v\n", err)
			return
		}
		defer file.Close()

		index := td.StripeID
		file.ReadAt(oldBuff, int64(index*config.ChunkSize))

		//xor
		for i := 0; i < len(buff); i++ {
			buff[i] = buff[i] ^ oldBuff[i]
		}
		//write to disk
		_, e := file.WriteAt(buff, int64(index*config.ChunkSize))
		if e != nil {
			log.Fatal("err: ", e)
		}

		//return ack
		ack := &config.ReqData{
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
func listen() {
	listenAddr := common.GetLocalIP()
	listenAddr = listenAddr + ":" + strconv.Itoa(config.ParityNodeListenPort)
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
			fmt.Printf("accept failed, err:%v", err)
			continue
		}
		//启动一个单独的goroutine去处理链接
		go handleReq(conn)
	}
}
