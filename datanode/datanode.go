package main

import (
	"EC/common"
	"EC/config"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
)

//handle req
func handleReq(conn net.Conn) {
	//targetIP := strings.Split(conn.RemoteAddr().String(),":")[0]

	defer conn.Close()
	//decode the req
	dec := gob.NewDecoder(conn)
	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
	}

	switch td.OPType {
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

		ack := &config.ReqData{
			ChunkID: td.DataChunkID,
			AckID:   td.DataChunkID + 1, //ackID=chunkID+1
		}
		fmt.Printf("The datanode updates success for chunk %d\n", td.DataChunkID)
		fmt.Printf("Sending ack back to client...\n")

		common.SendData(ack, config.ClientIP, config.ClientACKListenPort, "ack")

	}
}
func handleCMD(conn net.Conn)  {
	defer conn.Close()
	//decode the req
	dec := gob.NewDecoder(conn)
	var cmd config.CMD
	err := dec.Decode(&cmd)
	if err != nil {
		log.Fatal("handleReq:datanode更新数据，解码出错: ", err)
	}
	switch cmd.Type {

	case config.DDU:

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
			NextIPs: common.GetNeighborsIPs(common.GetRackID(common.GetLocalIP()), common.GetLocalIP()),
		}
		//send data to rootP
		common.SendData(sendData, cmd.ToIP, config.ParityListenPort, "")

	}
}
func handleACK(conn net.Conn) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)

	var ack config.Ack
	err := dec.Decode(&ack)
	if err != nil {
		log.Fatal("ms decoded error: ", err)
	}
	fmt.Printf("datanode received chunk %d's ack：%d\n",ack.ChunkID, ack.AckID)

	common.SendData(ack, config.MSIP, config.MSACKListenPort, "ack")
}

func main() {

	fmt.Printf("listening req in %s:%s\n",common.GetLocalIP(), config.NodeListenPort)
	l1, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeListenPort)
	if err != nil {
		fmt.Printf("listenReq failed, err:%v\n", err)
		return
	}

	fmt.Printf("listening cmd in %s:%s\n", common.GetLocalIP(), config.NodeCMDListenPort)
	l2, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeCMDListenPort)
	if err != nil {
		fmt.Printf("listenCMD failed, err:%v\n", err)
		return
	}

	fmt.Printf("listening ack in %s:%s\n", common.GetLocalIP(), config.NodeACKListenPort)
	l3, err := net.Listen("tcp", common.GetLocalIP() +  ":" + config.NodeACKListenPort)
	if err != nil {
		fmt.Printf("listenACK failed, err:%v\n", err)
		return
	}

	go listenCMD(l2)
	go listenACK(l3)
	listenReq(l1)

}
func listenReq(listen net.Listener) {

	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()
		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		handleReq(conn)


	}
}
func listenCMD(listen net.Listener) {

	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()

		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		go handleCMD(conn)

	}
}
func listenACK(listen net.Listener) {


	defer listen.Close()
	for {
		//等待客户端连接
		conn, err := listen.Accept()

		if err != nil {
			fmt.Printf("accept failed, err:%v\n", err)
			continue
		}
		go handleACK(conn)

	}

}




