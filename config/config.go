package config

import (
	"fmt"
	"github.com/templexxx/reedsolomon"
	"time"
)

const K int = 8
const M int = 4
const W int = 3
const N int = K + M
const NumOfRack int = 3
const BlockSize int = 1024 * 1024 //1MB
const Megabyte = 1024 * 1024      //1MB
const MaxBatchSize int = 100
const MaxBlockSize int = 1000000
const ECMode string = "RS" // or "XOR"
var CurPolicyVal = CAU
var CurPolicyStr = []string{"Base", "CAU", "Forest", "TUpdate"}
var OutFilePath = "../request/proj_4.csv.txt"
//var OutFilePath = "../request/proj_4.csv.bak.txt"
var BitMatrix = make([]byte, K*M*W*W)
const RackSize = M
const NumOfRacks = N / RackSize
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
	Forest
	T_Update

)
const BaseIP string = "192.168.1."
var MSIP = BaseIP + "108"
var ClientIP = BaseIP + "109"
const DataFilePath string = "../../test"
const StartIP int = 172
var NodeIPs =[N]string{
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
	SID                int            //
	StripeID           int
	BlockID            int             //如果有协作者ID，意思就是传XOR结果过去，blockID=第一条ID
	FromIP             string
	ToIPs              []string
	Helpers            []int            //协作者的blockIDs
	Matched            int             //已经匹配的blockID数量
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


func Init(){
	fmt.Printf("Init GM...\n")
	r, _ := reedsolomon.New(K, M)
	RS = r
	BitMatrix = GenerateBitMatrix(RS.GenMatrix, K, M, W)

}

func GenerateBitMatrix(matrix []byte, k, m, w int) []byte {
	bitMatrix := make([]byte, k*m*w*w)
	rowelts := k * w
	rowindex := 0

	for i := 0; i < m; i++ {
		colindex := rowindex
		for j := 0; j < k; j++ {
			elt := matrix[i*k+j]
			for x := 0; x < w; x++ {
				for l := 0; l < w; l++ {
					if (elt & (1 << l)) > 0 {
						bitMatrix[colindex+x+l*rowelts] = 1
					} else {
						bitMatrix[colindex+x+l*rowelts] = 0
					}
				}
				elt = Gfmul(elt, 2)
			}
			colindex += w
		}
		rowindex += rowelts * w
	}
	return bitMatrix

}
