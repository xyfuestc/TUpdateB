package ms

import (
	"EC/common"
	"EC/config"
	"EC/schedule"
	"fmt"
	"log"
	"net"
	"time"
)
var numOfReq = 0
var ackNum = 0
var curReqChunks = make([]config.MetaInfo, config.MaxBatchSize, config.MaxBatchSize)
var round = 0
var actualUpdatedBlocks = 0
var CurPolicy schedule.Policy = nil
var beginTime = time.Now()
//var bitMatrix = make(config.Matrix, 0, config.K * config.M * config.W * config.W)
func handleACK(conn net.Conn) {
	ack := common.GetACK(conn)
	ackNum++
	fmt.Printf("ms received chunk %d's ack：%d\n",ack.BlockID, ack.AckID)
	GetCurPolicy().HandleACK(ack)
	if schedule.IsEmptyInWaitingACKGroup() {
		fmt.Printf("=====================================")
		fmt.Printf("Simulation is done!")
		fmt.Printf("Total request: %d, spend time: %ds\n", numOfReq,
											time.Now().Unix() - beginTime.Unix())
		clearUpdates()
	}
}
func handleReq(conn net.Conn) {
	req := common.GetReq(conn)
	GetCurPolicy().HandleReq(req)
	numOfReq++
}
func handleWithOPType(req config.ReqData, conn net.Conn) {

}
func PrintGenMatrix(gm []byte)  {

	fmt.Printf("Generation Matrix : \n[")
	for i := 0; i < config.M; i++ {
		for j := 0; j < config.K; j++ {
			fmt.Printf("%d ", gm[i*config.K+j])

			if i==config.M-1 && j==config.K-1 {
				fmt.Printf("%d]", gm[i*config.K+j])
			}
		}
		fmt.Println()
	}
}
func clearUpdates() {
	fmt.Printf("clear all ranks info...\n")
	for _, rank := range config.Racks {
		rank.NumOfUpdates = 0
		rank.Stripes = make(map[int][]int)
	}
	ackNum = 0
	schedule.IsRunning = false
	// 考虑如果用户请求metainfo已经结束，无法启动CAU算法，则在每轮更新结束之后，启动CAU。
	//if len(CurPolicy.totalReqChunks) >= config.maxBatchSize && !schedule.IsRunning {
	//	schedule.NumOfCurNeedUpdateBlocks = 0
	//	schedule.CAU_Update(&totalReqChunks)
	//}
}
func printUpdatedStripes()  {
	var i = 0
	for _, rack := range config.Racks{
		fmt.Printf("rack %d = %v\n", i, rack.Stripes)
		i++
	}
}
func main() {
	/*init RS, nodes and racks*/
	config.Init()

	fmt.Printf("the ms is listening req: %s\n",config.MSListenPort) //8787
	l1, err := net.Listen("tcp", config.MSIP +":" + config.MSListenPort)
	fmt.Printf("the ms is listening ack: %s\n",config.NodeACKListenPort)  //8201
	l2, err := net.Listen("tcp", config.MSIP+ ":" + config.NodeACKListenPort)

	if err != nil {
		log.Fatal("ms listen err: ", err)
	}
	go listenACK(l2)
	listenReq(l1)
}
func listenReq(listen net.Listener) {
	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		handleReq(conn)
	}
}
func listenACK(listen net.Listener) {
	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:%v", err)
			continue
		}
		go handleACK(conn)
	}
}
func SetPolicy(policyType config.PolicyType)  {
	switch policyType {
	case config.BASE:
		CurPolicy = schedule.Base{}
	case config.CAU:
		CurPolicy = schedule.CAU{}
	case config.T_Update:
		CurPolicy = schedule.TUpdate{}
	case config.DPR_Forest:
		CurPolicy = schedule.Forest{}
	}
	CurPolicy.Init()
}
func GetCurPolicy() schedule.Policy {
	//init policy
	if CurPolicy == nil  {
		SetPolicy(config.CurPolicyVal)
	}
	return CurPolicy
}
func SetBeginTime(t time.Time)  {
	beginTime = t
}