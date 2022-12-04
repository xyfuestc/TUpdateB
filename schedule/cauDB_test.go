package schedule

import (
	"EC/config"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const (
	testNum = 10000
)

func TestAlign(t *testing.T)  {
	curDistinctReq = initReqs()
	stripes := turnReqsToStripes(curDistinctReq)
	beginTime := time.Now()
	for j, stripe := range stripes{
		for i := 0; i < config.NumOfRacks; i++ {
			if i != ParityRackIndex {

				//统一同一stripe的rangeL和rangeR
				alignRangeOfStripe(stripe)
				fmt.Printf("stripe %v: %v\n", j, stripe)
				//
				//if compareRacks(i, ParityRackIndex, stripe) {
				//	dxr_parityUpdate(i, stripe)
				//}else{
				//	dxr_dataUpdate(i, stripe)
				//}
			}
		}
	}
	fmt.Printf("数据块数量：%v, 对齐时间：%v\n", len(curDistinctReq), beginTime.String())
}

func initReqs() []*config.ReqData {
	reqs := make([]*config.ReqData, testNum)
	for i := 0; i < testNum; i++ {
		rangeL :=  rand.Int31n(int32(config.MaxBlockIndex))
		req := &config.ReqData{SID: i, BlockID: i, RangeLeft: int(rangeL), RangeRight: config.BlockSize}
		reqs[i] = req
	}
	return reqs
}

func TestPrim(t *testing.T)  {
	getAdjacentMatrix()
	path := GetMSTPath(relatedParityMatrix, nodeIndexs)

	//bPath := getBalancePath(path, nodeIndexs)
	//log.Printf("bPath : %v\n", bPath)
}
