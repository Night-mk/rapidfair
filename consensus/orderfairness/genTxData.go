package orderfairness

import (
	"math/rand"
	"strconv"
)

/*
生成交易数据 txList
生成模拟发送交易序列 sendTxSeq
测试
*/
type SimTxData struct {
	txNum   int
	nodeNum int
}

// nodeNum n和gamma的关系：n=(4*f/2*gamma-1) +1
func NewSimTxData(txNum int, nodeNum int) *SimTxData {
	return &SimTxData{
		txNum:   txNum,
		nodeNum: nodeNum,
	}
}

// 按指数分布（Exponential Distribution）生成随机变量
// 生成大小为k的符合E(1/u)指数分布的样本集，其中lambda=1/u，即lambda=0.5时，u=2
func ExpRandSample(u int, k int) []float64 {
	sampleResult := make([]float64, 0)
	for i := 0; i < k; i++ {
		// 使用rand库生成rate parameter=1指数分布随机变量
		// x := rand.ExpFloat64() / lambda
		x := rand.ExpFloat64() * float64(u)
		sampleResult = append(sampleResult, x)
	}
	return sampleResult
}

// 计算节点replica i（或client）的发送txNum交易的时间序列，指数分布随机生成
// u表示发送速率参数（单位时间发送数量）
func (st *SimTxData) sendTime(u int) []float64 {
	expRand := ExpRandSample(u, st.txNum)
	txSendTime := make([]float64, 0)
	currentTime := float64(0)
	for i := 0; i < st.txNum; i++ {
		currentTime += expRand[i]
		txSendTime = append(txSendTime, currentTime)
	}
	return txSendTime
}

// 计算交易tx到达节点replica i的延迟
func (st *SimTxData) recvTime(u int, r int) [][]float64 {
	// 构建二维矩阵，每行表示一个节点的到达延迟
	txRecvDelay := make([][]float64, 0)
	for i := 0; i < st.nodeNum; i++ {
		delayRand := ExpRandSample(u*r, st.txNum)
		txRecvDelay = append(txRecvDelay, delayRand)
		// 平均延迟 = sum(delayRand) / st.txNum
	}
	return txRecvDelay
}

// communication latency=send latency + receive latency
// 计算交易tx到达每个节点的实际时间点
func (st *SimTxData) commTime(txSendTime []float64, txRecvDelay [][]float64) [][]float64 {
	txCommTime := make([][]float64, 0)
	for i := 0; i < st.nodeNum; i++ {
		ctn := make([]float64, st.txNum)
		for j := 0; j < st.txNum; j++ {
			ctn[j] = txRecvDelay[i][j] + txSendTime[j]
		}
		txCommTime = append(txCommTime, ctn)
	}
	return txCommTime
}

// 按communication latency排序每个节点的交易序列，生成txList
func (st *SimTxData) memoList(txCommTime [][]float64) [][]string {
	txMemoList := make([][]string, 0)
	for _, item := range txCommTime {
		indexSort := Argsort(item) // 升序排序，并返回排序之前的索引
		tml := make([]string, st.txNum)
		for j, index := range indexSort {
			tml[j] = "tx" + strconv.Itoa(index)
		}
		txMemoList = append(txMemoList, tml)
	}
	return txMemoList
}

// 计算交易发送序列
func (st *SimTxData) sendTxSeq() []string {
	txSeq := make([]string, 0)
	for i := 0; i < st.txNum; i++ {
		txSeq = append(txSeq, "tx"+strconv.Itoa(i))
	}
	return txSeq
}

// 生成交易发送序列+每个节点的交易序列
func (st *SimTxData) GenSimTxData(u int, r int) ([][]string, []string) {
	txSendTime := st.sendTime(u)
	txRecvDelay := st.recvTime(u, r)
	txCommTime := st.commTime(txSendTime, txRecvDelay)

	txList := st.memoList(txCommTime)
	sendTxSeq := st.sendTxSeq()

	return txList, sendTxSeq
}

// 通用方法：升序排序，并返回排序之前的index
func Argsort(array []float64) []int {
	var n int = len(array)
	var index []int
	for i := 0; i < n; i++ {
		index = append(index, i)
	}
	// fmt.Println(index)
	// fmt.Println("数组array的长度为：", n)
	if n < 2 {
		return nil
	}
	for i := 1; i < n; i++ {
		// fmt.Printf("检查第%d个元素%f\t", i, array[i])
		var temp float64 = array[i]
		var tempIndex = index[i]
		var k int = i - 1
		for k >= 0 && array[k] > temp {
			k--
		}
		for j := i; j > k+1; j-- {
			array[j] = array[j-1]
			index[j] = index[j-1]
		}
		// fmt.Printf("其位置为%d\n", k+1)
		array[k+1] = temp
		index[k+1] = tempIndex
	}
	return index
}
