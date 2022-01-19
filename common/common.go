package common

import (
	"EC/config"
	"encoding/gob"
	"fmt"
	"github.com/dchest/uniuri"
	"github.com/wxnacy/wgo/arrays"
	"log"
	"math"
	"net"
	"os"
	"strings"
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
func GetIDFromIP(nodeIP string) int {
	for i,ip := range config.NodeIPs {
		if ip == nodeIP {
			return i
		}
	}
	return -1
	//return arrays.Contains(config.NodeIPs, nodeIP)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits
)

/*******发送数据*********/
func SendData(data interface{}, targetIP string, port string, retType string) interface{} {

	//1.与目标建立连接
	addr := fmt.Sprintf("%s:%s", targetIP, port)
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatal("common: SendData Dial error: ", err, " target: ", addr)
	}
	//2.发送数据
	enc := gob.NewEncoder(conn)
	err = enc.Encode(data)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatal("common: SendData gob encode error:  ", err, " target: ", addr)
	}

	return nil
}
func GetStripeIDFromBlockID(blockID int) int {
	return blockID/(config.K * config.W)
}

func GetRSStripeIDFromBlockID(blockID int) int {
	return blockID/config.K
}

//从0开始编号，一直到M*W-1
func RelatedParities(blockID int) []byte {
	parities := make([]byte, 0, config.M * config.W)
	col := blockID % (config.K * config.W)
	for i := col; i < len(config.BitMatrix); i += config.K * config.W {
		if config.BitMatrix[i] == 1 {
			parities = append(parities, (byte)(i/(config.K*config.W)))
		}
	}
	return parities
}
func RelatedParityNodes(parities []byte) []byte {
	nodes := make([]byte, 0, config.M)
	for i := 0; i < len(parities); i++ {
		nodeID := int(parities[i])/config.W + config.K
		if  arrays.Contains(nodes, byte(nodeID)) < 0 {
			nodes = append(nodes, byte(nodeID))
		}
	}
	return nodes
}

func GetRelatedParityIPs(blockID int) []string {
	parityIDs := RelatedParities(blockID)
	parityIPs := make([]string, 0, len(parityIDs))
	for _, parityID := range parityIDs {
		nodeID := int(parityID)/config.W + config.K
		nodeIP := config.NodeIPs[byte(nodeID)]
		if  arrays.Contains(parityIPs, nodeIP) < 0 {
			parityIPs = append(parityIPs, nodeIP)
		}
	}
	return parityIPs
}

func ReadBlock(blockID int) []byte  {
	index := GetIndex(blockID)
	//read data from disk
	var buff = make([]byte, config.BlockSize, config.BlockSize)
	file, err := os.OpenFile(config.DataFilePath, os.O_RDONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}
	defer file.Close()
	readSize, err := file.ReadAt(buff, int64(index*config.BlockSize))

	if err != nil {
		log.Fatal("读取文件失败：", err)
	}
	if readSize != config.BlockSize {
		log.Fatal("读取数据块失败！读取大小为：", readSize)
	}
	return buff
}
func ReadBlockWithSize(blockID, size int) []byte  {
	index := GetIndex(blockID)
	//read data from disk
	var buff = make([]byte, size, size)
	file, err := os.OpenFile(config.DataFilePath, os.O_RDONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}
	defer file.Close()
	readSize, err := file.ReadAt(buff, int64(index * size))

	if err != nil {
		log.Fatal("读取文件失败：", err)
	}
	if readSize != size {
		log.Fatal("读取数据块失败！读取大小为：", readSize)
	}
	return buff
}
func WriteBlock(blockID int, buff []byte)  {
	index := GetIndex(blockID)
	file, err := os.OpenFile(config.DataFilePath, os.O_WRONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}
	defer file.Close()
	_, err = file.WriteAt(buff, int64(index*config.BlockSize))
	log.Printf("write block %d done.\n", blockID)
}
func WriteBlockWithSize(blockID int, buff []byte, size int)  {
	index := GetIndex(blockID)
	file, err := os.OpenFile(config.DataFilePath, os.O_WRONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}
	defer file.Close()
	_, err = file.WriteAt(buff, int64(index * size))
	log.Printf("write block %d with size: %dB done .\n", blockID, size)
}
func GetNodeID(blockID int) int {
	return blockID % (config.K * config.W) / config.W
}
func GetNodeIP(nodeID int) string {
	return config.NodeIPs[nodeID]
}
func RandWriteBlockAndRetDelta(blockID int) []byte  {
	//newDataStr := RandStringBytesMaskImpr(config.NumOfMB)
	newDataStr := uniuri.NewLen(config.BlockSize)

	newBuff := []byte(newDataStr)
	/*****read old data*******/
	oldBuff := ReadBlock(blockID)
	/*****compute new delta data*******/
	deltaBuff := make([]byte, config.BlockSize, config.BlockSize)
	for i := 0; i < len(newBuff); i++ {
		deltaBuff[i] = newBuff[i] ^ oldBuff[i]
	}
	/*****write new data*******/
	go WriteBlock(blockID, newBuff)

	return deltaBuff
}
func WriteDeltaBlock(blockID int, deltaBuff []byte) []byte  {
	size := len(deltaBuff)
	/*****read old data*******/
	oldBuff := ReadBlockWithSize(blockID, size)
	/*****compute new delta data*******/
	newBuff := make([]byte, size)
	for i := 0; i < size; i++ {
		newBuff[i] = deltaBuff[i] ^ oldBuff[i]
	}
	/*****write new data*******/
	WriteBlockWithSize(blockID, newBuff, size)
	//fmt.Printf("成功写入blockID: %d, size: %d!\n", blockID, size)

	return deltaBuff
}


func GetCMD(conn net.Conn) config.CMD  {
	dec := gob.NewDecoder(conn)
	var cmd config.CMD
	err := dec.Decode(&cmd)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatal("GetCMD : Decode error: ", err)

	}
	conn.Close()
	return cmd
}
func SendCMD(fromIP string, toIPs []string, sid, blockID int)  {
	cmd := &config.CMD{
		SID: sid,
		BlockID: blockID,
		ToIPs: toIPs,
		FromIP: fromIP,
		SendSize: config.BlockSize,
		Helpers: make([]int, 0, 1),
		Matched: 0,
	}
	SendData(cmd, fromIP, config.NodeCMDListenPort, "")
}

func SendCMDWithHelpers(fromIP string, toIPs []string, sid, blockID int, helpers []int)  {
	cmd := &config.CMD{
		SID: sid,
		BlockID: blockID,
		ToIPs: toIPs,
		FromIP: fromIP,
		SendSize: config.BlockSize,
		Helpers: helpers,
		Matched: 0,
	}
	SendData(cmd, fromIP, config.NodeCMDListenPort, "")
}

func GetACK(conn net.Conn) config.ACK {
	dec := gob.NewDecoder(conn)

	var ack config.ACK
	err := dec.Decode(&ack)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatalln("GetACK : Decode error: ", err)

	}
	fmt.Printf("received block %d's ack from %s, sid: %d\n", ack.BlockID, GetConnIP(conn), ack.SID)
	conn.Close()
	return ack
}
func GetTD(conn net.Conn) config.TD {
	//defer conn.Close()
	dec := gob.NewDecoder(conn)
	var td config.TD
	err := dec.Decode(&td)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatalln("GetTD from ", GetConnIP(conn), "result in decoding error: ", err, "blockID: ", td.BlockID, "sid: ", td.SID)
	}
	conn.Close()
	return td
}
func GetPolicy(conn net.Conn) config.Policy  {
	//defer conn.Close()
	dec := gob.NewDecoder(conn)
	var p config.Policy
	err := dec.Decode(&p)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatalln("GetPolicy from ", GetConnIP(conn), "result in decoding error: ", err)
	}
	conn.Close()
	return p
}
func GetBlocksFromOneRequest(userRequest config.UserRequest) (int,int)  {
	minBlockID := userRequest.AccessOffset / config.BlockSize
	maxBlockID := int(math.Ceil(float64((userRequest.AccessOffset+userRequest.OperatedSize)*1.0 / config.BlockSize)))

	return minBlockID, maxBlockID
}
func GetConnIP(conn net.Conn) string  {
	addr := conn.RemoteAddr().String()
	ip := strings.Split(addr, ":")[0]
	return ip
}
func GetIndex(blockID int) int {
	return int(math.Min(float64(blockID/(config.K*config.W)), float64(config.MaxBlockIndex)))
}
//求并集
func Union(slice1, slice2 []int) []int {
	m := make(map[int]int)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times, _ := m[v]
		if times == 0 {
			slice1 = append(slice1, v)
		}
	}
	return slice1
}
func GetParityIDFromIndex(i int) int {
	return config.K + i / config.W
}
func GetDataNodeIDFromIndex(rackID, i int) int {
	return rackID * config.RackSize + i
}

func Delete(Array, indexes []int)  {
	//删除
	for i:= 0; i < len(Array);{
		if j := arrays.Contains(indexes, Array[i]) ; j >= 0 {
			//Array = append(Array[:i], Array[i+1:]...)
			Array[i], Array[len(Array)-1] = Array[len(Array)-1], Array[i]
			Array = Array[:len(Array)-1]

			//indexes = append(indexes[:j], indexes[j+1:]...)
			indexes[j], indexes[len(indexes)-1] = indexes[len(indexes)-1], indexes[j]
			indexes = indexes[:len(indexes)-1]
		}else{
			i++
		}

	}
	fmt.Printf("Delete: Array = %v\n", Array)
}
