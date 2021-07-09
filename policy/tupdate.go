package policy

import (
	"EC/common"
	"EC/config"
	"fmt"
	"github.com/wxnacy/wgo/arrays"
	"sort"
)

type Graph struct {
	N   int //顶点数
	M   int //边数
	Arc [][]byte
}

type Task struct {
	Start byte
	End   byte
	BlockID int
}

func TaskAdjust(taskGroup []Task)  {
	for _, t := range taskGroup {
		s, e :=  t.Start, t.End
		if s > e {
			t.Start, t.End = t.End, t.Start
		}
	}
}




type TUpdate struct {
	
}

func (p TUpdate) Init()  {
	InitNetworkDistance()
}

const MAX_COUNT int = 9
const INFINITY byte = 255
var nodeMatrix = make(config.Matrix, (config.K+config.M)*(config.K+config.M))
func InitNetworkDistance()  {
	//缺一个网络距离矩阵
	for i := 0; i < config.K+config.M; i++ {
		for j := 0; j < config.K+config.M; j++ {
			if i == j {
				nodeMatrix[i*(config.K+config.M)+j] = 0
			}else{
				nodeMatrix[i*(config.K+config.M)+j] = 5
			}
		}
	}
	nodeMatrix[0*(config.K+config.M)+1] = 1
	nodeMatrix[1*(config.K+config.M)+0] = 1
	nodeMatrix[2*(config.K+config.M)+3] = 1
	nodeMatrix[3*(config.K+config.M)+2] = 1
	nodeMatrix[4*(config.K+config.M)+5] = 1
	nodeMatrix[5*(config.K+config.M)+4] = 1
	nodeMatrix[6*(config.K+config.M)+7] = 1
	nodeMatrix[7*(config.K+config.M)+6] = 1
	nodeMatrix[8*(config.K+config.M)+9] = 1
	nodeMatrix[9*(config.K+config.M)+8] = 1
}
/*Prim算法*/
func Prim(G Graph) config.Matrix{
	var lowCost = [MAX_COUNT]byte{}
	var vertex = make(config.Matrix, MAX_COUNT)
	lowCost[0] = 0
	for j := 1; j < G.N; j++ {
		lowCost[j] = G.Arc[0][j]
		vertex[j] = 0
	}
	for i := 1; i < G.N; i++ {
		k := 1
		min := INFINITY
		for j := 1; j < G.N; j++ {
			if lowCost[j] != 0 && lowCost[j] < min {
				min = lowCost[j]
				k = j
			}
		}
		fmt.Printf("(%d, %d) ", vertex[k], k)

		lowCost[k] = 0
		for j := 0; j < G.N; j++ {
			if lowCost[j] != 0 && G.Arc[k][j] < lowCost[j] {
				lowCost[j] = G.Arc[k][j]
				vertex[j] = byte(k)
			}
		}
	}
	return vertex
}
/*构造函数*/
func NewGraph(N int) Graph {
	buf := make([][]byte, N)
	for i := 0; i < N; i++ {
		buf[i] = make([]byte, N)
	}
	return Graph{
		N: N,
		M: 0,
		Arc: buf,
	}
}
/*测试*/
func TestFunc(matrix, nodeIndexs config.Matrix) config.Matrix   {
	len := len(nodeIndexs)
	G := NewGraph(len)
	for i := 0; i < G.N; i++ {
		for j := 0; j < G.N; j++ {
			G.Arc[i][j] = matrix[i*len+j]
		}
	}
	path := Prim(G)
	return path
}


func T_Update(blockID int) []Task {
	parities :=	common.GetRelatedParities(blockID)
	fmt.Printf("%v\n", parities)
	nodeID := common.GetNodeID(blockID)
	fmt.Printf("%v\n", nodeMatrix)
	relatedParityMatrix, nodeIndexs := getAdjacentMatrix(parities, nodeID, nodeMatrix)
	fmt.Printf("%v\n", relatedParityMatrix)
	fmt.Printf("%v\n", nodeIndexs)
	path := TestFunc(relatedParityMatrix, nodeIndexs)
	fmt.Printf("path: %v\n", path)
	taskGroup := make([]Task, 0, len(nodeIndexs)-1)
	for i := 1; i < len(nodeIndexs); i++ {
		//fmt.Printf("(%d, %d) ", path[i], i)
		fmt.Printf("(%d, %d) ", nodeIndexs[path[i]], nodeIndexs[i])
		taskGroup = append(taskGroup, Task{Start: nodeIndexs[path[i]], BlockID: blockID, End:nodeIndexs[i]})
	}
	TaskAdjust(taskGroup)
	sort.SliceStable(taskGroup, func(i, j int) bool {
		return taskGroup[i].Start < taskGroup[j].Start
	})

	fmt.Printf("taskGroup:%v\n", taskGroup)

	return taskGroup

}
func getAdjacentMatrix(parities []byte, nodeID int, allMatrix []byte) (config.Matrix, config.Matrix) {
	nodeIndexes := make(config.Matrix, 0, (1+config.M)*(1+config.M))
	nodeIndexes = append(nodeIndexes, (byte)(nodeID/config.W))
	for i := 0; i < len(parities); i++ {
		if arrays.Contains(nodeIndexes, parities[i]/(byte)(config.W)) < 0 {
			nodeIndexes = append(nodeIndexes, parities[i]/(byte)(config.W))
		}
	}
	len := len(nodeIndexes) //[0 4 5]
	newMatrix := make(config.Matrix, len*len)
	for i := 0; i < len; i++ {
		for j := 0; j < len; j++ {
			value :=  nodeIndexes[i]*(byte)(config.K+config.M)+ nodeIndexes[j]
			newMatrix[i*len+j] = allMatrix[value]
		}
	}
	return newMatrix, nodeIndexes
}




