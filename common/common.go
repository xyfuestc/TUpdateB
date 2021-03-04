package common

import (
	"EC/config"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
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
func SendData(data interface{}, targetIP string, port int, retType string) interface{} {

	//1.与目标建立连接
	addr := fmt.Sprintf("%s:%d", targetIP, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal("接收数据出错: ", err)
	}
	//2.发送数据
	enc := gob.NewEncoder(conn)
	err = enc.Encode(data)
	if err != nil {
		log.Fatal("发送数据出错: ", err)
	}

	switch retType {
	case "metaInfo":
		retData := config.MetaInfo{}

		//3.接收返回数据
		dec := gob.NewDecoder(conn)
		err = dec.Decode(&retData)
		if err != nil {
			log.Fatal("接收数据出错: ", err)
		}

		return retData
	case "ack":
		retData := config.ReqData{}

		//3.receive ack
		dec := gob.NewDecoder(conn)
		err = dec.Decode(&retData)
		if err != nil {
			log.Fatal("接收数据出错: ", err)
		}

		return retData
	default:
		return nil
	}

	return nil
}

/**********get IP from nodeID**********/
func GetNodeIP(nodeID int) string {
	if nodeID <= config.K {
		return config.DataNodeIPs[nodeID]
	} else {
		return config.ParityNodeIPs[nodeID-config.K]
	}

}

func IsContain(items []int, item int) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func GetParityIDFromIP(ip string) int {

	for i := 0; i < 3; i++ {
		if  config.Rack2.Nodes[strconv.Itoa(i)] == ip{
			return i
		}
	}
	return -1
}
