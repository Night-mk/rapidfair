package orderfairness

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func InitGraph() *Graph {
	g := NewGraph()
	g.addVertex("tx1")
	g.addVertex("tx2")
	g.addVertex("tx3")
	g.addVertex("tx4")
	g.addVertex("tx5")
	g.addVertex("tx6")

	g.addEdge("tx1", "tx2")
	g.addEdge("tx1", "tx3")
	g.addEdge("tx2", "tx5")
	g.addEdge("tx3", "tx5")
	g.addEdge("tx5", "tx6")
	g.addEdge("tx4", "tx1")

	return g
}

// 通过测试
func TestGraph(t *testing.T) {
	g := InitGraph()
	g.Print()
	// t.Logf("%v", "add edge tx6,tx1")
	// g.addEdge("tx6", "tx1") // 增加边 （完成）
	// g.Print()

	// t.Logf("%v", "delete edge tx5,tx6") // 删除边（完成）
	// g.deleteEdge("tx5", "tx6")
	// g.Print()

	// t.Logf("%v", "add vertex tx9") // 增加顶点 （完成）
	// g.addVertex("tx9")
	// g.Print()

	// t.Logf("%v", "delete vertex tx6") // 删除顶点 （完成）
	// g.deleteVertex("tx6")
	// g.Print()

	verts := g.getVertices() // 获取所有顶点
	fmt.Print(verts)

	for _, u := range verts {
		if len(g.edgeList[u]) == 0 {
			fmt.Print(u)
		}
	}

	var vt = []string{"tx1", "tx2"}
	u := vt[0]
	vt = vt[1:]
	fmt.Println("\n value of u: ", u)
	fmt.Println("arr: ", vt)
}

var U int = 1
var R int = 1
var TxNum int = 50    // 50, 100, 200, 400
var NodeNum int = 5   // 5, 6, 9, 21, 41
var Gamma float32 = 1 // 1, 0.9, 0.75, 0.6, 0.55
// var Fault int = 3
var Rounds int = 2

// 测试公平排序交易序列
func TestOrderFairness(t *testing.T) {
	sumExTime := uint64(0)
	allExTime := make([]uint64, 0)
	fmt.Println("Node num: ", NodeNum)
	fmt.Println("gamma: ", Gamma)
	// 测试k次取平均值（5次？）
	for k := 0; k < Rounds; k++ {
		fmt.Println("Round ", k)
		// 1. 生成txList
		// 生成txList的节点数量为quorum
		quorumNodeNum := NodeNum - int(math.Floor((float64(nodeNum-1)*float64(2*Gamma-1))/float64(4)))
		// fmt.Println(quorumNodeNum)
		st := NewSimTxData(TxNum, quorumNodeNum)
		txList, sendTxSeq := st.GenSimTxData(U, R)
		// fmt.Println("txList: ", txList)
		fmt.Println("sendTxSeq len: ", len(sendTxSeq))
		// fmt.Println(sendTxSeq)

		// 2. 测试公平排序
		// 计算程序执行时间
		start := time.Now() // 获取当前时间
		finalTxSeq := FairOrder_Themis(txList, NodeNum, Gamma)
		elapsed := time.Since(start)
		fmt.Println("Execution time:", elapsed)
		sumExTime += uint64(elapsed)
		allExTime = append(allExTime, uint64(elapsed))
		// fmt.Println(float64(uint64(elapsed)) / float64(1000000))
		fmt.Println("finalTxSeq len: ", len(finalTxSeq))
		// fmt.Println("finalTxSeq: ", finalTxSeq)

		sRate := Metric_SuccessOrderRate(finalTxSeq, sendTxSeq)
		fmt.Printf("success order rate: %.4f \n", sRate)
	}

	// 计算平均值
	avgExTime := float64(sumExTime) / float64(1000000*Rounds)
	// 计算标准差 = sqrt(sum((x-avg)^2)/num)
	vari := float64(0)
	for _, x := range allExTime {
		// fmt.Println("vari: ", float64(x)/float64(1000000)-avgExTime)
		vari += math.Pow(float64(x)/float64(1000000)-avgExTime, 2)
	}
	stdev := math.Sqrt(vari / float64(Rounds))
	fmt.Printf("\n Avg order exec time: %.4f ± %.4f ms \n", avgExTime, stdev)
	// 计算平均偏差
	// for _, x := range allExTime {
	// 	vari += math.Abs(float64(x)/float64(1000000*Rounds) - avgExTime)
	// }
	// avgVari := vari / float64(Rounds)
	// fmt.Printf("\n Avg order exec time: %.4f ± %.4f ms \n", avgExTime, avgVari)
}
