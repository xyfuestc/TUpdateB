package common

import (
	"EC/config"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"github.com/wxnacy/wgo/arrays"
	"os"
)

var kinds = map[string]func() interface{}{
	"metaInfo": func() interface{} { return &config.MetaInfo{} },
	"ack":      func() interface{} { return &config.ReqData{} },
}

func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		return err.Error()
	}
	for _, value := range addrs {
		if ipnet, ok := value.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "IP获取失败"
}

func GetChunkIP(chunkGID int) string {
	nodeID := chunkGID - (chunkGID/config.K)*config.K
	return GetNodeIP(nodeID)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
)

func RandStringBytesMask(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; {
		if idx := int(rand.Int63() & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i++
		}
	}
	return string(b)
}

/*******发送数据*********/
func SendData(data interface{}, targetIP string, port string, retType string) interface{} {

	//1.与目标建立连接
	addr := fmt.Sprintf("%s:%s", targetIP, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal("common: SendData Dial error: ", err)
	}
	//2.发送数据
	enc := gob.NewEncoder(conn)
	err = enc.Encode(data)
	if err != nil {
		log.Fatal("common: SendData gob encode error:  ", err)
	}

	//switch retType {
	////case "metaInfo":
	//	retData := config.MetaInfo{}
	//
	//	//3.接收返回数据
	//	dec := gob.NewDecoder(conn)
	//	err = dec.Decode(&retData)
	//	if err != nil {
	//		log.Fatal("common: SendData of case metaInfo gob decode error", err)
	//	}
	//
	//	return retData
	//
	//default:
	//	return nil
	//}

	return nil
}

/**********get IP from nodeID**********/


func IsContain(items []int, item int) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func IsContainB(items config.Matrix, item byte) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func GetParityIDFromIP(ip string) int {

	//for i := 0; i < 3; i++ {
	//	if  config.Racks[2].Nodes[i] == ip{
	//		return i
	//	}
	//}
	return -1
}


func GetRackID(ip string) int {
	for i, rack := range config.Racks{

		for _, v := range rack.Nodes {
			if v == ip {
				return i
			}
		}
	}
	return -1
}

func GetRackIDFromNode(nodeID int) int {
	rackSize := (config.K+config.M)/config.M
	return nodeID / rackSize
}

func GetNeighborsIPs(rackID int, ip string) []string {
	if rackID < 0 {
		log.Fatal("GetNeighborsIPs error: rackID < 0")
		return nil
	}
	ips := config.Racks[rackID].Nodes
	var neighbors = make([]string, 0, config.M)
	for _, v := range ips {
		if v == ip {    //except self
			continue
		}
		neighbors = append(neighbors, v)
	}
	return neighbors
}

func GetStripeIDFromBlockID(blockID int) int {
	return blockID/(config.K * config.W)
}
/*获取data相关的parity*/
/*
	P0 = [1 2 3 4]
	P1 = [0 1 2 5]
	P2 = [0 1 3 4]
	P3 = [0 2 3 5]
*/
func GetRelatedParities(blockID int) config.Matrix  {
	parities := make(config.Matrix, 0, config.W*config.M)
	//var j byte  //row index
	//j = byte(config.K * config.W)
	//for i := blockID; i < len(bitMatrix); i+=config.K*config.W {
	//	if bitMatrix[i] == 1 {
	//		parities = append(parities, j)
	//	}
	//	j++
	//}
	nodeID := blockID%config.K
	switch nodeID {
	case 0:
		parities = append(parities, 1, 2, 3)
	case 1:
		parities = append(parities, 0, 1, 2)
	case 2:
		parities = append(parities, 0, 1, 3)
	case 3:
		parities = append(parities, 0, 2, 3)
	case 4:
		parities = append(parities, 0, 2)
	case 5:
		parities = append(parities, 1, 3)
	default:
		log.Fatal("GetRelatedParities error: nodeID < 0")
	}
	for i := 0; i < len(parities); i++ {
		parities[i] += byte(config.K)
	}

	return parities
}
func GetRelatedParityIPs(blockID int) []string {
	parityIDs := GetRelatedParities(blockID)
	parityIPs := make([]string, 0, len(parityIDs))
	for _, index := range parityIDs {
		curIP := config.ParityNodeIPs[(int(index))/config.W]
		if arrays.ContainsString(parityIPs, curIP) < 0 {
			parityIPs = append(parityIPs, curIP)
		}
	}
	return parityIPs
}

func ReadBlock(blockID int) []byte  {
	index := blockID /config.K
	//read data from disk
	var buff = make([]byte, config.ChunkSize, config.ChunkSize)
	file, err := os.OpenFile(config.DataFilePath, os.O_RDONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}
	defer file.Close()
	readSize, err := file.ReadAt(buff, int64((index-1)*config.ChunkSize))

	if err != nil {
		log.Fatal("读取文件失败：", err)
	}
	if readSize != config.ChunkSize {
		log.Fatal("读取数据块失败！读取大小为：", readSize)
	}
	return buff
}
func WriteBlock(blockID int, buff []byte)  {
	index := blockID /config.K
	file, err := os.OpenFile(config.DataFilePath, os.O_WRONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}
	defer file.Close()
	_, err = file.WriteAt(buff, int64((index-1)*config.ChunkSize))

}
func GetNodeID(blockID int) int {
	if blockID < 0 {
		 return -1
	}
	return blockID%config.K
}
func GetStripeID(blockID int) int  {
	if blockID < 0 {
		return -1
	}
	return blockID/config.K
}
func GetNodeIP(blockID int) string  {
	nodeID := GetNodeID(blockID)
	if nodeID < 0 {
		return "error"
	}
	if nodeID <= config.K {
		return config.DataNodeIPs[nodeID]
	} else {
		return config.ParityNodeIPs[nodeID-config.K]
	}
}
func GetNodeIPFromNodeID(nodeID int) string {
	//parity node
	if nodeID > config.K - 1 {
		 return config.ParityNodeIPs[nodeID - config.K]
	}else{  //data node
		return config.DataNodeIPs[nodeID]
	}
}
func RandWriteBlockAndRetDelta(blockID int) []byte  {
	newDataStr := RandStringBytesMask(config.ChunkSize)
	newBuff := []byte(newDataStr)
	/*****read old data*******/
	oldBuff := ReadBlock(blockID)
	/*****compute new delta data*******/
	deltaBuff := make([]byte, config.ChunkSize, config.ChunkSize)
	for i := 0; i < len(newBuff); i++ {
		deltaBuff[i] = newBuff[i] ^ oldBuff[i]
	}
	/*****write new data*******/
	WriteBlock(blockID, newBuff)

	return deltaBuff
}

