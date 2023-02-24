package fairorderpb

// RapidFair: convert 转换proto数据和hotstuff处理使用的数据类型

import (
	"github.com/relab/hotstuff"
)

// 将输入数据转化proto类型的消息
func TxSeqToProto(col hotstuff.CollectTxSeq) *CollectTxSeq {
	// 获取当前的view ID（baseline中collect阶段的viewID和共识相同）
	// 将交易列表Command转为bytes
	// serData, err := json.Marshal(col.TxSeq())
	// if err != nil {
	// 	fmt.Printf("TxSeqToProto serialization error: err=%v", err)
	// }
	return &CollectTxSeq{
		View:  uint64(col.View()),
		TxSeq: []byte(col.TxSeq()),
	}
}

// 将proto数据转换为hotstuff中可以处理的数据结构
func TxSeqFromProto(col *CollectTxSeq) hotstuff.CollectTxSeq {
	// 转byte到string
	// var unserTxSeq string
	// err := json.Unmarshal(col.GetTxSeq(), &unserTxSeq)
	// if err != nil {
	// 	fmt.Printf("TxSeqFromProto unserialization error: err=%v", err)
	// }
	command := hotstuff.Command(col.GetTxSeq())
	return hotstuff.NewCollectTxSeq(hotstuff.View(col.GetView()), command)
}
