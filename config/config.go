package config

import "github.com/templexxx/reedsolomon"

const K int = 6
const M int = 3
const W int = 1

const ChunkSize int = 1024 * 1024 //1MB
const MaxBatchSize int = 100

type OPType int
//type CMDType int

const (
	MSListenPort   int = 8977   // metainfo server listening port
	MSACKListenPort   int = 8978   // metainfo server ack listening port

	NodeListenPort int = 8979 // datanode or paritynode listening port
	ParityNodeListenPort int = 9979
	NodeACKListenPort   int = 8980   // metainfo server ack listening port

	ClientACKListenPort int = 8981
)

//DataNode操作
const (
	//data operation
	UpdateReq      OPType = iota //client update, 0
	SendDataToRoot               //内部发送数据，1
	DDURoot                      //data发送给parity，2
	DDULeaf
	PDU


	//metaserver cmd
	DDU // DDU(i < j)，发送命令给DataNode，使其转发更新数据给rootParity


	//ack
	ACK
)


type Strategy int

const (
	CAU Strategy = 0
)
//const BaseIP string = "172.19.0."
const BaseIP string = "192.168.1."
//const MSIP = BaseIP + "3"
var MSIP = BaseIP + "1"
//const DataFilePath string = "/tmp/dataFile.dt"
const DataFilePath string = "../data/dataFile"
const StartIP int = 172
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
//type CMD struct {
//	SendSize           int
//	Type               CMDType
//	StripeID           int
//	DataChunkID        int
//	UpdateParityID     int
//	NumRecvChunkItem   int
//	NumRecvChunkParity int
//	PortNum            int
//	ToIP             string
//	FromIP             string
//	ToIP               string
//}
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
	SeqNum  int
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
	Nodes        map[string]string
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
