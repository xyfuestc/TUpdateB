package config

import (
	"github.com/templexxx/reedsolomon"
	"strconv"
)

const K int = 6
const M int = 3
const W int = 1

const ChunkSize int = 1024 * 1024 //1MB
const MaxBatchSize int = 100

type OPType int
//type CMDType int

const (
	MSListenPort   string = "8787"   // metainfo server listening port
	MSACKListenPort   string = "8201"   // metainfo server ack listening port

	NodeListenPort      string = "8300" // datanode or paritynode listening port
	ParityListenPort    string = "8303"
	ParityACKListenPort string = "8304"
	NodeACKListenPort   string = "8301"   // metainfo server ack listening port
	NodeCMDListenPort   string = "8302"   // metainfo server ack listening port

	ClientACKListenPort string = "8400"
)

//DataNode操作
const (
	//data operation
	UpdateReq      OPType = iota //client update, 0
	SendDataToRoot               //内部发送数据，1
	DDURoot                      //data发送给parity，2
	DDULeaf

	PDU

	//ack
	ACK
)



type CMDType int

const (
	//data operation
	DDU      CMDType = iota //client update, 0


)

type Role int
const (
	UknownRole Role = iota
	DDURootPRole
	DDULeafPRole

)


type Strategy int

const (
	CAU Strategy = 0
)
//const BaseIP string = "172.19.0."
const BaseIP string = "192.168.1."
//const MSIP = BaseIP + "3"
var MSIP = BaseIP + "172"
var ClientIP = BaseIP + "170"
//const DataFilePath string = "/tmp/dataFile.dt"
const DataFilePath string = "../../test"
const StartIP int = 173
var DataNodeIPs = [K]string{}
var ParityNodeIPs = [M]string{}
var Racks = [K+M/M]Rack{}


//传输数据格式
type TD struct {
	SendSize           int
	OPType             OPType
	StripeID           int
	DataChunkID        int
	UpdateParityID     int
	NumRecvChunkItem   int
	NumRecvChunkParity int
	PortNum            int
	ToIP               string
	SenderIP           string
	FromIP             string
	NextIPs            []string
	Buff               []byte
}


//传输命令格式
type CMD struct {
	SendSize           int
	Type               CMDType
	StripeID           int
	DataChunkID        int
	UpdateParityID     int
	NumRecvChunkItem   int
	NumRecvChunkParity int
	PortNum            int
	FromIP             string
	ToIP               string
}
type ReqData struct {
	OPType  OPType
	ChunkID int
	AckID   int
}

type ReqType struct {
	Type OPType
}

type Ack struct {
	AckID   int
	ChunkID int
}

type MetaInfo struct {
	StripeID        int
	DataChunkID     int
	ChunkStoreIndex int //chunkID
	RelatedParities []string
	ChunkIP         string
	DataNodeID      int
	RackID          int
}

type UpdateStripe struct {
	StripeID  int
	DataIDs   []int
	ParityIDs []int
}
type Rack struct {
	Nodes        []string
	NodeNum      int
	CurUpdateNum int
	Stripes      map[int][]int
	GateIP       string
}

var RS *reedsolomon.RS

//获取数据块（chunkID）对应的IP
func GetRelatedParities(chunkID int) []string {
	var relatedParities []string = make([]string, RS.ParityNum)
	col := chunkID % RS.DataNum

	for i := 0; i < RS.ParityNum; i++ {
		if RS.GenMatrix[i*RS.DataNum+col] > 0 {
			relatedParities = append(relatedParities, ParityNodeIPs[i])
		}
	}
	return relatedParities
}
func getRackID(dataNodeID int) int {
	if dataNodeID < 3 {
		return 0
	} else {
		return 1
	}
}
func InitNodesRacks(){
	//init Nodes and Racks
	var start  = StartIP
	for g := 0; g < len(DataNodeIPs); g++ {
		strIP := BaseIP + strconv.FormatInt(int64(start), 10)
		DataNodeIPs[g] = strIP
		start++
	}

	for g := 0; g < len(ParityNodeIPs); g++ {
		strIP := BaseIP + strconv.FormatInt(int64(start), 10)
		ParityNodeIPs[g] = strIP
		start++
	}

	start = StartIP

	for g := 0; g < len(Racks); g++ {
		strIP1 := BaseIP + strconv.FormatInt(int64(start), 10)
		strIP2 := BaseIP + strconv.FormatInt(int64(start+1), 10)
		strIP3 := BaseIP + strconv.FormatInt(int64(start+2), 10)
		//strIP1 := BaseIP + strconv.FormatInt(int64(start), 10)
		//strIP2 := BaseIP + strconv.FormatInt(int64(start), 10)
		//strIP3 := BaseIP + strconv.FormatInt(int64(start), 10)
		Racks[g] = Rack{
			Nodes:        []string{strIP1, strIP2, strIP3},
			NodeNum:      3,
			CurUpdateNum: 0,
			Stripes:      map[int][]int{},
			GateIP:       "",
		}
		start++
	}
}