package config

import (
	"github.com/templexxx/reedsolomon"
	"log"
	"runtime"
	"sync"
)

const K int = 8
const M int = 4
const W int = 4
const N int = K + M
var NumOfMB = 64
var BlockSize = Megabyte * NumOfMB //1MB
var RSBlockSize = Megabyte * NumOfMB * W
const Megabyte = 1024 * 1024      //1MB
const MaxBatchSize int = 100
const MaxRSBatchSize int = 100
const MaxBaseBatchSize int = 100
const MaxBlockSize int = 1000
const TestFileSize = 10 * 1024 * Megabyte
var MaxBlockIndex = TestFileSize / BlockSize - 1
const NumOfAlgorithm int = 8 //采用3种算法执行相同任务
var CurPolicyStr = []string{"Base", "CAU", "TUpdate", "TUpdate1", "TAR_CAU", "CAU1", "BaseMulticast", "CAURS" }
var BitMatrix = make([]byte, K*M*W*W)
const RackSize = M
const NumOfRacks = N / RackSize
type OPType int
var RS *reedsolomon.RS
type Matrix []byte
const MulticastAddr = "224.0.0.250"
//const MulticastAddrWithPort = "224.0.0.250:9981"
const MulticastAddrWithPort = "224.0.0.1:9999"
const MulticastAddrListenACK  = ":9981"
const MTUSize = 4 * 1024 // 4K
const MaxDatagramSize = 8 * 1024 // 8 * 1024 = 8KB

const (
	BASE PolicyType = iota
	CAU
	T_Update
	T_Update1
	TAR_CAU
	CAU1
	BASEMulticast
	CAURS
)
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

type Policy struct {
	Type      int
	NumOfMB   int
	TraceName string
	Multicast bool
}

const (
	NodeReqListenPort       string = "8300" // datanode or paritynode listening port
	NodeACKListenPort       string = "8301"   // metainfo server ack listening port
	NodeCMDListenPort       string = "8302"   // metainfo server ack listening port
	NodeSettingsListenPort  string = "8303"   // metainfo server ack listening port
	NodeTDListenPort        string = "8304"   // metainfo server ack listening port
)

type PolicyType int


const BaseIP string = "192.168.1."
var MSIP = BaseIP + "108"
const DataFilePath string = "../../test"
const StartIP int = 172
var NodeIPs =[]string{
	BaseIP+"110", BaseIP+"111", BaseIP+"112", BaseIP+"113",     //rack0
	BaseIP+"120", BaseIP+"121", BaseIP+"122", BaseIP+"123",     //rack1
	BaseIP+"130", BaseIP+"131", BaseIP+"132", BaseIP+"133",     //rack2
}
//共享池
var TDBufferPool sync.Pool
var CMDBufferPool sync.Pool
var ReqBufferPool sync.Pool
var AckBufferPool sync.Pool
//var XORBlockBufferPool sync.Pool
var BlockBufferPool sync.Pool
var NumOfWorkers int


//传输数据格式
type TD struct {
	SID                int
	BlockID            int
	SendSize           int
	ToIP               string
	FromIP             string
	Buff               []byte
	MultiTargetIPs     []string
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
	SendSize           int
}
type ReqData struct {
	SID         int
	BlockID     int
	AckID       int
	StripeID    int
	RangeLeft   int
	RangeRight  int
}

type ReqType struct {
	Type OPType
}

type MTU struct {
	SID            int      `json:"sid,omitempty"`
	BlockID        int      `json:"block_id,omitempty"`
	Index          int      `json:"index,omitempty"`
	Data           []byte   `json:"data,omitempty"`
	FromIP         string   `json:"from_ip,omitempty"`
	MultiTargetIPs []string `json:"multi_target_ips,omitempty"`
	IsFragment     bool     `json:"is_fragment,omitempty"`
	FragmentID     int      `json:"fragment_id,omitempty"`
	FragmentCount  int      `json:"fragment_count,omitempty"`
	SendSize       int      `json:"send_size,omitempty"`
}


type ACK struct {
	SID         int
	AckID       int
	BlockID     int
	FragmentID  int
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

func Init(){

	log.Printf("Init (K, M, W) and XOR relations...\n")
	r, _ := reedsolomon.New(K, M)
	RS = r
	BitMatrix = GenerateBitMatrix(RS.GenMatrix, K, M, W)

	//init num of go workers
	NumOfWorkers = runtime.NumCPU()

	log.Printf("初始化共享池...\n")
	InitBufferPool()
}
func InitBufferPool()  {
	ReqBufferPool = sync.Pool{
		New: func() interface{} {
			return new(ReqData)
		},
	}
	AckBufferPool = sync.Pool{
		New: func() interface{} {
			return new(ACK)
		},
	}
	CMDBufferPool = sync.Pool{
		New: func() interface{} {
			return new(CMD)
		},
	}
	TDBufferPool = sync.Pool{
		New: func() interface{} {
			return new(TD)
		},
	}
	//XORBlockBufferPool = sync.Pool{
	//	New: func() interface{} {
	//		return make([]byte, BlockSize)
	//	},
	//}
	BlockBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, RSBlockSize)
		},
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