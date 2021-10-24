package config

import (
	"EC/common"
	"fmt"
	"github.com/templexxx/reedsolomon"
	"time"
)

const K int = 8
const M int = 4
const W int = 3
const NumOfRack int = 3
const ChunkSize int = 1024 * 1024 //1MB
const MaxBatchSize int = 100
const ECMode string = "RS" // or "XOR"
var CurPolicyVal = BASE
var OutFilePath = "../request/proj_4.csv.txt"


type OPType int
/******the structure of one line for the update stream file*******/
const (
	Timestamp int  = iota    // default 0
	WorkloadName    //1
	VolumeID        //2
	OperationType   //3
	AccessOffset    //4
	OperatedSize    //5
	DurationTime    //6
)
type UserRequest struct {
	Timestamp       uint64
	WorkloadName    string
	VolumeID        int
	OperationType   string
	AccessOffset    int
	OperatedSize    int
	DurationTime    int
}

const (
	NodeReqListenPort   string = "8300" // datanode or paritynode listening port
	NodeACKListenPort   string = "8301"   // metainfo server ack listening port
	NodeCMDListenPort   string = "8302"   // metainfo server ack listening port
	NodeTDListenPort   string = "8304"   // metainfo server ack listening port
)

//DataNode操作
const (
	/*********base data operation**********/
	OP_BASE         OPType = iota //client update, 0
	/*********cau data operation**********/
	UpdateReq
	SendDataToRoot               //内部发送数据，1
	DDURoot                      //data发送给parity，2
	DDULeaf

	PDU

)

type CMDType int

const (
	//data operation
	CMD_DDU CMDType = iota //client update, 0
	CMD_BASE

)

type Role int
const (
	Role_Uknown Role = iota
)


type PolicyType int

const (
	BASE PolicyType = iota
	CAU
	T_Update
	DPR_Forest

)
const BaseIP string = "192.168.1."
var MSIP = BaseIP + "108"
var ClientIP = BaseIP + "109"
const DataFilePath string = "../../test"
const StartIP int = 172
var NodeIPs =[K+M]string{
	BaseIP+"110", BaseIP+"111", BaseIP+"112", BaseIP+"113",     //rack0
	BaseIP+"120", BaseIP+"121", BaseIP+"122", BaseIP+"123",     //rack1
	BaseIP+"140", BaseIP+"141", BaseIP+"142", BaseIP+"143",     //rack2
}
var Racks = [NumOfRack]Rack{}
var BeginTime = time.Now()

//传输数据格式
type TD struct {
	SendSize           int
	OPType             OPType
	StripeID           int
	BlockID            int
	ToIP               string
	SenderIP           string
	FromIP             string
	NextIPs            []string
	Buff               []byte
	SID                int
}

//传输命令格式
type CMD struct {
	CreatorIP          string
	SID                int
	SendSize           int
	Type               CMDType
	StripeID           int
	BlockID            int
	UpdateParityID     int
	NumRecvChunkItem   int
	NumRecvChunkParity int
	PortNum            int
	FromIP             string
	ToIPs              []string
	ToOneIP            string
}
type ReqData struct {
	SID      int
	//OPType   OPType
	BlockID  int
	AckID    int
	StripeID int
}

type ReqType struct {
	Type OPType
}

type ACK struct {
	SID     int
	AckID   int
	BlockID int
}

type WaitingACKItem struct {
	SID             int
	BlockID         int
	RequiredACK     int
	ACKReceiverIP   string
	ACKSenderIP     string
}

type MetaInfo struct {
	StripeID         int
	BlockID          int
	ChunkStoreIndex  int
	RelatedParityIPs []string
	BlockIP          string
	DataNodeID       int
	RackID           int
	SectionID        int
}

type UpdateStripe struct {
	StripeID  int
	DataIDs   []int
	ParityIDs []int
}
type Rack struct {
	Nodes        []string
	NodeNum      int
	NumOfUpdates int
	Stripes      map[int][]int
	GateIP       string
}

var RS *reedsolomon.RS

type Matrix []byte
const INFINITY = 65535
var BitMatrix = make([]byte, K*M*W*W)

func Init(){
	//1.init GM, get BitMatrix
	fmt.Printf("Init GM...\n")
	r, _ := reedsolomon.New(K, M)
	RS = r
	BitMatrix = common.GenerateBitMatrix(RS.GenMatrix, K, M, W)

	////3.init racks
	//for g := 0; g < len(Racks); g++ {
	//	strIP1 := BaseIP + strconv.FormatInt(int64(start), 10)
	//	strIP2 := BaseIP + strconv.FormatInt(int64(start+1), 10)
	//	strIP3 := BaseIP + strconv.FormatInt(int64(start+2), 10)
	//	Racks[g] = Rack{
	//		Nodes:        []string{strIP1, strIP2, strIP3},
	//		NodeNum:      3,
	//		NumOfUpdates: 0,
	//		Stripes:      map[int][]int{},
	//		GateIP:       "",
	//	}
	//	start+=3
	//	fmt.Printf("Rack %d has nodes: %s, %s, %s\n", g, strIP1, strIP2, strIP3)
	//}
}

