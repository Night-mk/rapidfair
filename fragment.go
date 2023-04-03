package hotstuff

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// RapidFair: 为并行的公平排序序列构建Fragment暂存已经完成公平排序的交易

type Fragment struct {
	hash      Hash
	virView   View    // 解耦的公平排序的view编号
	virLeader ID      // virtual view对应的leader
	orderedTx Command // 公平排序的交易序列G
	txSeq     TXList  // 缓存交易序列TxSeq的实际数据
	missEdge  Command // 公平排序对上一个virtual view没有提交交易的丢失序列
	updateSeq TXList  // 缓存更新序列UpdateSeq的实际数据
}

func NewFragment(virView View, virLeader ID, orderedTx Command, txSeq TXList, missEdge Command, updateSeq TXList) *Fragment {
	f := &Fragment{
		virView:   virView,
		virLeader: virLeader,
		orderedTx: orderedTx,
		txSeq:     txSeq,
		missEdge:  missEdge,
		updateSeq: updateSeq,
	}
	f.hash = sha256.Sum256(f.ToBytes())
	return f
}

func (f *Fragment) String() string {
	return fmt.Sprintf(
		"Fragment{vir view: %d, vir leader: %d}",
		f.virView,
		f.virLeader,
	)
}

// 返回Fragment私有变量的值
func (f *Fragment) Hash() Hash {
	return f.hash
}

func (f *Fragment) VirView() View {
	return f.virView
}

func (f *Fragment) VirLeader() ID {
	return f.virLeader
}

func (f *Fragment) OrderedTx() Command {
	return f.orderedTx
}

func (f *Fragment) TxSeq() TXList {
	return f.txSeq
}

func (f *Fragment) MissEdge() Command {
	return f.missEdge
}

func (f *Fragment) UpdateSeq() TXList {
	return f.updateSeq
}

// 序列化对象为[]byte
func (f *Fragment) ToBytes() []byte {
	var buf []byte
	var viewBuf [8]byte
	binary.LittleEndian.PutUint64(viewBuf[:], uint64(f.virView))
	buf = append(buf, viewBuf[:]...)
	var leaderBuf [4]byte
	binary.LittleEndian.PutUint32(leaderBuf[:], uint32(f.virLeader))
	buf = append(buf, leaderBuf[:]...)
	buf = append(buf, []byte(f.orderedTx)...)
	buf = append(buf, f.txSeq.ToBytes()...)
	buf = append(buf, []byte(f.missEdge)...)
	buf = append(buf, f.updateSeq.ToBytes()...)
	return buf
}

// 维护:交易序列哈希TxSeqHash
type TxSeqFragment struct {
	parent    Hash
	hash      Hash
	cert      QuorumCert
	virView   View // 解耦的公平排序的view编号
	virLeader ID   // virtual view对应的leader
	// txSeqHash TxSeqHash // 交易序列哈希 Map<ID, hash(Command[])>
	txSeqHash TXList
}

func NewTxSeqFragment(parent Hash, cert QuorumCert, virView View, virLeader ID, txSeqHash TXList) *TxSeqFragment {
	t := &TxSeqFragment{
		parent:    parent,
		cert:      cert,
		virView:   virView,
		virLeader: virLeader,
		txSeqHash: txSeqHash,
	}
	t.hash = sha256.Sum256(t.ToBytes())
	return t
}

func (t *TxSeqFragment) String() string {
	return fmt.Sprintf(
		"Fragment{vir view: %d, vir leader: %d}",
		t.virView,
		t.virLeader,
	)
}

func (t *TxSeqFragment) Hash() Hash {
	return t.hash
}

func (t *TxSeqFragment) Parent() Hash {
	return t.parent
}

func (t *TxSeqFragment) QuorumCert() QuorumCert {
	return t.cert
}

func (t *TxSeqFragment) VirView() View {
	return t.virView
}

func (t *TxSeqFragment) VirLeader() ID {
	return t.virLeader
}

func (t *TxSeqFragment) TxSeqHash() TXList {
	return t.txSeqHash
}

func (t *TxSeqFragment) ToBytes() []byte {
	buf := t.parent[:]
	var viewBuf [8]byte
	binary.LittleEndian.PutUint64(viewBuf[:], uint64(t.virView))
	buf = append(buf, viewBuf[:]...)
	var leaderBuf [4]byte
	binary.LittleEndian.PutUint32(leaderBuf[:], uint32(t.virLeader))
	buf = append(buf, leaderBuf[:]...)
	buf = append(buf, t.txSeqHash.ToBytes()...)

	return buf
}

// Fragment的核心数据，对txSeq，updateSeq做hash
type FragmentData struct {
	hash      Hash
	virView   View    // 解耦的公平排序的view编号
	orderedTx Command // 公平排序的交易序列G
	txSeq     Hash    // 缓存交易序列TxSeq的实际数据
	missEdge  Command // 公平排序对上一个virtual view没有提交交易的丢失序列
	updateSeq Hash    // 缓存更新序列UpdateSeq的实际数据
}

func NewFragmentData(virView View, orderedTx Command, txSeq Hash, missEdge Command, updateSeq Hash) *FragmentData {
	f := &FragmentData{
		virView:   virView,
		orderedTx: orderedTx,
		txSeq:     txSeq,
		missEdge:  missEdge,
		updateSeq: updateSeq,
	}
	f.hash = sha256.Sum256(f.ToBytes())
	return f
}

func (f *FragmentData) Hash() Hash {
	return f.hash
}

func (f *FragmentData) VirView() View {
	return f.virView
}

func (f *FragmentData) OrderedTx() Command {
	return f.orderedTx
}

func (f *FragmentData) TxSeq() Hash {
	return f.txSeq
}

func (f *FragmentData) MissEdge() Command {
	return f.missEdge
}

func (f *FragmentData) UpdateSeq() Hash {
	return f.updateSeq
}

func (f *FragmentData) ToBytes() []byte {
	var buf []byte
	var viewBuf [8]byte
	binary.LittleEndian.PutUint64(viewBuf[:], uint64(f.virView))
	buf = append(buf, viewBuf[:]...)
	buf = append(buf, []byte(f.orderedTx)...)
	buf = append(buf, f.txSeq[:]...)
	buf = append(buf, []byte(f.missEdge)...)
	buf = append(buf, f.updateSeq[:]...)
	return buf
}
