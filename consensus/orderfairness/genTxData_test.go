package orderfairness

import (
	"fmt"
	"testing"
)

// 测试：按排序返回索引
func TestArgSort(t *testing.T) { // 通过测试
	arr := []float64{
		float64(1), float64(3), float64(9), float64(6), float64(2),
	}

	arrId := Argsort(arr)
	fmt.Println(arrId)
}

// 测试: 生成交易发送序列+每个节点的交易序列
var u int = 1
var r int = 1
var txNum int = 10
var nodeNum int = 4

func TestGenData(t *testing.T) {
	st := NewSimTxData(txNum, nodeNum)
	txList, sendTxSeq := st.GenSimTxData(u, r)
	fmt.Println("txList: ")
	fmt.Println(txList)
	fmt.Println("sendTxSeq: ")
	fmt.Println(sendTxSeq)
}
