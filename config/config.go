package config

import (
	"fmt"
	"github.com/templexxx/reedsolomon"
	"strconv"
	"time"
)

const K int = 6
const M int = 4
const W int = 1
const NumOfRack int = 5
const MaxReqSize = 100000
const ChunkSize int = 1024 * 1024 //1MB
const MaxBatchSize int = 100
const ECMode string = "RS" // or "XOR"
const MaxNumOfBlocks int = 1000000
const MaxNumOfRequests int = 1000000
var CurPolicyVal = BASE

type OPType int
//type CMDType int

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
	MSListenPort   string = "8787"   // metainfo server listening port
	MSACKListenPort   string = "8201"   // metainfo server ack listening port
	NodeReqListenPort   string = "8300" // datanode or paritynode listening port
	NodeACKListenPort   string = "8301"   // metainfo server ack listening port
	NodeCMDListenPort   string = "8302"   // metainfo server ack listening port
	NodeTDListenPort   string = "8304"   // metainfo server ack listening port
	ClientACKListenPort string = "8400"
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
	/*********forest data operation**********/
	OP_DPR

	/*********T_Update data operation**********/
	OP_T_Update

	//ack
	//ACK
)

type CMDType int

const (
	//data operation
	CMD_DDU CMDType = iota //client update, 0
	CMD_BASE

	//tupdate
	CMD_TUpdate
)

type Role int
const (
	Role_Uknown Role = iota
	Role_DDURootP
	Role_DDULeafP

)


type PolicyType int

const (
	BASE PolicyType = iota
	CAU
	T_Update
	DPR_Forest

)
//const BaseIP string = "172.19.0."
const BaseIP string = "192.168.1."
//const MSIP = BaseIP + "3"
var MSIP = BaseIP + "171"
//var MSIP = "127.0.0.1"
//var ClientIP = BaseIP + "170"
//const DataFilePath string = "/tmp/dataFile.dt"
const DataFilePath string = "../../test"
const StartIP int = 173
var DataNodeIPs = [K]string{}
var ParityNodeIPs = [M]string{}
var Racks = [NumOfRack]Rack{}
var BeginTime = time.Now()
//var BitMatrix = make(Matrix, 0, K*M*W*W*K*M*W*W)

//传输数据格式
type TD struct {
	SendSize           int
	OPType             OPType
	StripeID           int
	BlockID            int
	//UpdateParityID     int
	//NumRecvChunkItem   int
	//NumRecvChunkParity int
	//PortNum            int
	ToIP               string
	SenderIP           string
	FromIP             string
	NextIPs            []string
	Buff               []byte
	SID                int
}


//传输命令格式
type CMD struct {
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
	OPType   OPType
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

func Init(){
	//1.init GM, get BitMatrix
	fmt.Printf("Init GM...\n")
	r, _ := reedsolomon.New(K, M)
	RS = r
	//BitMatrix = GenerateBitMatrix(RS.GenMatrix, K, M, W)

	//2.init Nodes IP and Racks IP
	fmt.Printf("Init nodes and racks...\n")
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

	//3.init racks
	for g := 0; g < len(Racks); g++ {
		strIP1 := BaseIP + strconv.FormatInt(int64(start), 10)
		strIP2 := BaseIP + strconv.FormatInt(int64(start+1), 10)
		strIP3 := BaseIP + strconv.FormatInt(int64(start+2), 10)
		Racks[g] = Rack{
			Nodes:        []string{strIP1, strIP2, strIP3},
			NodeNum:      3,
			NumOfUpdates: 0,
			Stripes:      map[int][]int{},
			GateIP:       "",
		}
		start+=3
		fmt.Printf("Rack %d has nodes: %s, %s, %s\n", g, strIP1, strIP2, strIP3)
	}
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
