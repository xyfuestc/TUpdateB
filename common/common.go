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
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits
)

/*******发送数据*********/
func SendData(data interface{}, targetIP string, port string) {

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
	conn.Close()
}
func GetStripeIDFromBlockID(blockID int) int {
	return blockID/(config.K * config.W)
}

func GetRSStripeIDFromBlockID(blockID int) int {
	return blockID/config.K
}

//统计blockID与哪些parityID有关（一共有M*W个parityID）
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

func ReadBlockWithSize(blockID, size int) []byte  {
	index := GetIndex(blockID)
	buff := config.BlockBufferPool.Get().([]byte)

	len := len(buff)
	if len < size {
		for i := 0; i < size - len; i++ {
			buff = append(buff, byte(0))
		}
	}

	file, err := os.OpenFile(config.DataFilePath, os.O_RDONLY, 0)

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}

	defer file.Close()

	readSize, err := file.ReadAt(buff[:size], int64(index * size))

	if err != nil {
		log.Fatal("读取文件失败：", err)
	}
	if readSize != size {
		log.Printf("读取大小为不一致 in ReadBlockWithSize：%+v, %+v", readSize, size)
	}

	return buff[:size]
}
func WriteBlockWithSize(blockID int, buff []byte, size int)  {
	index := GetIndex(blockID)
	file, err := os.OpenFile(config.DataFilePath, os.O_WRONLY, 0)

	defer file.Close()

	if err != nil {
		log.Fatalln("打开文件出错: ", err)
	}

	_, err = file.WriteAt(buff[:size], int64(index * size))
}
func GetNodeID(blockID int) int {
	return blockID % (config.K * config.W) / config.W
}
func GetNodeIP(nodeID int) string {
	return config.NodeIPs[nodeID]
}
func RandWriteBlockAndRetDelta(blockID, size int) []byte  {
	newDataStr := uniuri.NewLen(size)
	newBuff := config.BlockBufferPool.Get().([]byte)

	len := len(newBuff)
	if len < size {
		for i := 0; i < size - len; i++ {
			newBuff = append(newBuff, newDataStr[i])
		}
	}

	copy(newBuff, newDataStr)
	/*****read old data*******/
	oldBuff := ReadBlockWithSize(blockID, size)
	/*****compute new delta data*******/
	deltaBuff := config.BlockBufferPool.Get().([]byte)
	for i := 0; i < size; i++ {
		//deltaBuff[i] = newBuff[i] ^ oldBuff[i]
		deltaBuff = append(deltaBuff, newBuff[i] ^ oldBuff[i])
	}

	/*****write new data*******/
	WriteBlockWithSize(blockID, newBuff, size)

	//释放空间
	config.BlockBufferPool.Put(oldBuff)
	config.BlockBufferPool.Put(newBuff)

	return deltaBuff[:size]
}
func WriteDeltaBlock(blockID int, deltaBuff []byte)   {
	size := len(deltaBuff)
	/*****read old data*******/
	oldBuff := ReadBlockWithSize(blockID, size)
	/*****compute new delta data*******/
	newBuff := config.BlockBufferPool.Get().([]byte)

	for i := 0; i < size; i++ {
		//newBuff[i] =
		newBuff = append(newBuff, deltaBuff[i] ^ oldBuff[i])
	}
	/*****write new data*******/
	WriteBlockWithSize(blockID, newBuff, size)

	//释放空间
	config.BlockBufferPool.Put(oldBuff)
	config.BlockBufferPool.Put(newBuff)
}


func GetCMD(conn net.Conn) config.CMD  {
	defer conn.Close()

	dec := gob.NewDecoder(conn)
	var cmd config.CMD
	//cmd := config.CMDBufferPool.Get().(*config.CMD)
	err := dec.Decode(&cmd)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatal("GetCMD : Decode error: ", err)

	}
	return cmd
}
func SendCMD(fromIP string, toIPs []string, sid, blockID int)  {

	cmd := config.CMDBufferPool.Get().(*config.CMD)
	cmd.SID = sid
	cmd.BlockID = blockID
	cmd.SendSize = config.BlockSize
	cmd.Helpers = make([]int, 0, 1)
	cmd.Matched = 0
	cmd.ToIPs = toIPs
	cmd.FromIP = fromIP

	SendData(cmd, fromIP, config.NodeCMDListenPort)

	config.CMDBufferPool.Put(cmd)
}

func SendCMDWithHelpers(fromIP string, toIPs []string, sid, blockID int, helpers []int)  {

	cmd := config.CMDBufferPool.Get().(*config.CMD)
	cmd.SID = sid
	cmd.BlockID = blockID
	cmd.SendSize = config.BlockSize
	cmd.Helpers = helpers
	cmd.Matched = 0
	cmd.ToIPs = toIPs
	cmd.FromIP = fromIP

	SendData(cmd, fromIP, config.NodeCMDListenPort)

	config.CMDBufferPool.Put(cmd)
}

func GetACK(conn net.Conn) config.ACK {
	defer 	conn.Close()
	dec := gob.NewDecoder(conn)

	var ack config.ACK
	err := dec.Decode(&ack)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatalln("GetACK : Decode error: ", err)

	}
	log.Printf("received block %d's ack from %s, sid: %d\n", ack.BlockID, GetConnIP(conn), ack.SID)
	return ack
}
func GetTD(conn net.Conn) config.TD {
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	var td config.TD
	//td := config.TDBufferPool.Get().(*config.TD)
	err := dec.Decode(&td)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatalln("GetTD from ", GetConnIP(conn), "result in decoding error: ", err, "blockID: ", td.BlockID, "sid: ", td.SID)
	}
	return td
}
func GetPolicy(conn net.Conn) config.Policy  {
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	var p config.Policy
	err := dec.Decode(&p)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		log.Fatalln("GetPolicy from ", GetConnIP(conn), "result in decoding error: ", err)
	}
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

func PrintError(errMsg string, err error) {
	if err != nil {
		log.Fatalln(errMsg, err.Error())
	}
}
func PrintMessage(data config.MTU) {
	fmt.Println("=== Data received ===")
	fmt.Println("SID: ", data.SID)
	fmt.Println("BlockID: ", data.BlockID)
	fmt.Println("FromIP:", data.FromIP)
	fmt.Println("MultiTargetIPs:", data.MultiTargetIPs)
	fmt.Println("FragmentCount:", data.FragmentCount)
	fmt.Println("IsFragment:", data.IsFragment)
	//fmt.Println("= Data =")
	//fmt.Println("Content:", data.Data)
}

func StringConcat(A, split, B string) string  {
	var s strings.Builder
	s.WriteString(A)
	s.WriteString(split)
	s.WriteString(B)

	return s.String()
}