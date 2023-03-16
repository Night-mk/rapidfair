package hotstuffpb

import (
	fmt "fmt"
	"math/big"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/crypto"
	"github.com/relab/hotstuff/crypto/bls12"
	"github.com/relab/hotstuff/crypto/ecdsa"
	"github.com/relab/hotstuff/internal/proto/clientpb"
	"google.golang.org/protobuf/proto"
)

// QuorumSignatureToProto converts a threshold signature to a protocol buffers message.
func QuorumSignatureToProto(sig hotstuff.QuorumSignature) *QuorumSignature {
	signature := &QuorumSignature{}
	switch s := sig.(type) {
	case ecdsa.MultiSignature:
		sigs := make([]*ECDSASignature, 0, len(s))
		for _, p := range s {
			sigs = append(sigs, &ECDSASignature{
				Signer: uint32(p.Signer()),
				R:      p.R().Bytes(),
				S:      p.S().Bytes(),
			})
		}
		signature.Sig = &QuorumSignature_ECDSASigs{ECDSASigs: &ECDSAMultiSignature{
			Sigs: sigs,
		}}
	case *bls12.AggregateSignature:
		signature.Sig = &QuorumSignature_BLS12Sig{BLS12Sig: &BLS12AggregateSignature{
			Sig:          s.ToBytes(),
			Participants: s.Bitfield().Bytes(),
		}}
	}
	return signature
}

// QuorumSignatureFromProto converts a protocol buffers message to a threshold signature.
func QuorumSignatureFromProto(sig *QuorumSignature) hotstuff.QuorumSignature {
	if signature := sig.GetECDSASigs(); signature != nil {
		sigs := make([]*ecdsa.Signature, len(signature.GetSigs()))
		for i, sig := range signature.GetSigs() {
			r := new(big.Int)
			r.SetBytes(sig.GetR())
			s := new(big.Int)
			s.SetBytes(sig.GetS())
			sigs[i] = ecdsa.RestoreSignature(r, s, hotstuff.ID(sig.GetSigner()))
		}
		return ecdsa.RestoreMultiSignature(sigs)
	}
	if signature := sig.GetBLS12Sig(); signature != nil {
		aggSig, err := bls12.RestoreAggregateSignature(signature.GetSig(), crypto.BitfieldFromBytes(signature.GetParticipants()))
		if err != nil {
			return nil
		}
		return aggSig
	}
	return nil
}

// PartialCertToProto converts a consensus.PartialCert to a hotstuffpb.Partialcert.
func PartialCertToProto(cert hotstuff.PartialCert) *PartialCert {
	hash := cert.BlockHash()
	return &PartialCert{
		Sig:  QuorumSignatureToProto(cert.Signature()),
		Hash: hash[:],
	}
}

// PartialCertFromProto converts a hotstuffpb.PartialCert to an ecdsa.PartialCert.
func PartialCertFromProto(cert *PartialCert) hotstuff.PartialCert {
	var h hotstuff.Hash
	copy(h[:], cert.GetHash())
	return hotstuff.NewPartialCert(QuorumSignatureFromProto(cert.GetSig()), h)
}

// QuorumCertToProto converts a consensus.QuorumCert to a hotstuffpb.QuorumCert.
func QuorumCertToProto(qc hotstuff.QuorumCert) *QuorumCert {
	hash := qc.BlockHash()
	return &QuorumCert{
		Sig:  QuorumSignatureToProto(qc.Signature()),
		Hash: hash[:],
		View: uint64(qc.View()),
	}
}

// QuorumCertFromProto converts a hotstuffpb.QuorumCert to an ecdsa.QuorumCert.
func QuorumCertFromProto(qc *QuorumCert) hotstuff.QuorumCert {
	var h hotstuff.Hash
	copy(h[:], qc.GetHash())
	return hotstuff.NewQuorumCert(QuorumSignatureFromProto(qc.GetSig()), hotstuff.View(qc.GetView()), h)
}

// ProposalToProto converts a ProposeMsg to a protobuf message.
func ProposalToProto(proposal hotstuff.ProposeMsg) *Proposal {
	p := &Proposal{
		Block: BlockToProto(proposal.Block),
	}
	if proposal.AggregateQC != nil {
		p.AggQC = AggregateQCToProto(*proposal.AggregateQC)
	}
	return p
}

// ProposalFromProto converts a protobuf message to a ProposeMsg.
func ProposalFromProto(p *Proposal) (proposal hotstuff.ProposeMsg) {
	proposal.Block = BlockFromProto(p.GetBlock())
	if p.GetAggQC() != nil {
		aggQC := AggregateQCFromProto(p.GetAggQC())
		proposal.AggregateQC = &aggQC
	}
	return
}

// BlockToProto converts a consensus.Block to a hotstuffpb.Block.
func BlockToProto(block *hotstuff.Block) *Block {
	parentHash := block.Parent()
	// RapidFair: baseline 将TxSeq map转为[]bytes (使用proto的marshal)
	txSeqB := []byte{}
	err := error(nil)
	if len(block.TxSeq()) > 0 {
		mapbatch := new(clientpb.MapBatch)
		// 对MapBatch的MapTxSeq进行初始化
		if len(mapbatch.MapTxSeq) == 0 {
			mapbatch.MapTxSeq = make(map[uint32][]byte)
		}
		marshaler := proto.MarshalOptions{Deterministic: true}
		for k, v := range block.TxSeq() {
			mapbatch.MapTxSeq[uint32(k)] = []byte(v)
		}
		txSeqB, err = marshaler.Marshal(mapbatch)
		if err != nil {
			fmt.Printf("[BlockToProto]: TxSeq serialization error: err=%v", err)
		}
	}
	// RapidFair: END

	return &Block{
		Parent:   parentHash[:],
		Command:  []byte(block.Command()),
		QC:       QuorumCertToProto(block.QuorumCert()),
		View:     uint64(block.View()),
		Proposer: uint32(block.Proposer()),
		TxSeq:    txSeqB, // 增加TxSeq字段
	}
}

// BlockFromProto converts a hotstuffpb.Block to a consensus.Block.
func BlockFromProto(block *Block) *hotstuff.Block {
	var p hotstuff.Hash
	copy(p[:], block.GetParent())
	// RapidFair: baseline 反序列化TxSeq, []bytes转为map[uint32][]byte
	mapbatch := new(clientpb.MapBatch)
	unmarshaler := proto.UnmarshalOptions{DiscardUnknown: true}
	err := unmarshaler.Unmarshal(block.GetTxSeq(), mapbatch)
	if err != nil {
		fmt.Printf("[BlockFromProto]: TxSeq unserialization error: err=%v", err)
	}
	txSeq := make(map[hotstuff.ID]hotstuff.Command)
	if len(mapbatch.GetMapTxSeq()) > 0 {
		for k, v := range mapbatch.GetMapTxSeq() {
			txSeq[hotstuff.ID(k)] = hotstuff.Command(string(v))
		}
	}
	// RapidFair: END
	if len(txSeq) > 0 { // order-fairness block
		return hotstuff.NewFairBlock(
			p,
			QuorumCertFromProto(block.GetQC()),
			hotstuff.Command(block.GetCommand()),
			hotstuff.View(block.GetView()),
			hotstuff.ID(block.GetProposer()),
			txSeq,
		)
	} else {
		return hotstuff.NewBlock(
			p,
			QuorumCertFromProto(block.GetQC()),
			hotstuff.Command(block.GetCommand()),
			hotstuff.View(block.GetView()),
			hotstuff.ID(block.GetProposer()),
		)
	}
}

// TimeoutMsgFromProto converts a TimeoutMsg proto to the hotstuff type.
func TimeoutMsgFromProto(m *TimeoutMsg) hotstuff.TimeoutMsg {
	timeoutMsg := hotstuff.TimeoutMsg{
		View:          hotstuff.View(m.GetView()),
		SyncInfo:      SyncInfoFromProto(m.GetSyncInfo()),
		ViewSignature: QuorumSignatureFromProto(m.GetViewSig()),
	}
	if m.GetViewSig() != nil {
		timeoutMsg.MsgSignature = QuorumSignatureFromProto(m.GetMsgSig())
	}
	return timeoutMsg
}

// TimeoutMsgToProto converts a TimeoutMsg to the protobuf type.
func TimeoutMsgToProto(timeoutMsg hotstuff.TimeoutMsg) *TimeoutMsg {
	tm := &TimeoutMsg{
		View:     uint64(timeoutMsg.View),
		SyncInfo: SyncInfoToProto(timeoutMsg.SyncInfo),
		ViewSig:  QuorumSignatureToProto(timeoutMsg.ViewSignature),
	}
	if timeoutMsg.MsgSignature != nil {
		tm.MsgSig = QuorumSignatureToProto(timeoutMsg.MsgSignature)
	}
	return tm
}

// TimeoutCertFromProto converts a timeout certificate from the protobuf type to the hotstuff type.
func TimeoutCertFromProto(m *TimeoutCert) hotstuff.TimeoutCert {
	return hotstuff.NewTimeoutCert(QuorumSignatureFromProto(m.GetSig()), hotstuff.View(m.GetView()))
}

// TimeoutCertToProto converts a timeout certificate from the hotstuff type to the protobuf type.
func TimeoutCertToProto(timeoutCert hotstuff.TimeoutCert) *TimeoutCert {
	return &TimeoutCert{
		View: uint64(timeoutCert.View()),
		Sig:  QuorumSignatureToProto(timeoutCert.Signature()),
	}
}

// AggregateQCFromProto converts an AggregateQC from the protobuf type to the hotstuff type.
func AggregateQCFromProto(m *AggQC) hotstuff.AggregateQC {
	qcs := make(map[hotstuff.ID]hotstuff.QuorumCert)
	for id, pQC := range m.GetQCs() {
		qcs[hotstuff.ID(id)] = QuorumCertFromProto(pQC)
	}
	return hotstuff.NewAggregateQC(qcs, QuorumSignatureFromProto(m.GetSig()), hotstuff.View(m.GetView()))
}

// AggregateQCToProto converts an AggregateQC from the hotstuff type to the protobuf type.
func AggregateQCToProto(aggQC hotstuff.AggregateQC) *AggQC {
	pQCs := make(map[uint32]*QuorumCert, len(aggQC.QCs()))
	for id, qc := range aggQC.QCs() {
		pQCs[uint32(id)] = QuorumCertToProto(qc)
	}
	return &AggQC{QCs: pQCs, Sig: QuorumSignatureToProto(aggQC.Sig()), View: uint64(aggQC.View())}
}

// SyncInfoFromProto converts a SyncInfo struct from the protobuf type to the hotstuff type.
func SyncInfoFromProto(m *SyncInfo) hotstuff.SyncInfo {
	si := hotstuff.NewSyncInfo()
	if qc := m.GetQC(); qc != nil {
		si = si.WithQC(QuorumCertFromProto(qc))
	}
	if tc := m.GetTC(); tc != nil {
		si = si.WithTC(TimeoutCertFromProto(tc))
	}
	if aggQC := m.GetAggQC(); aggQC != nil {
		si = si.WithAggQC(AggregateQCFromProto(aggQC))
	}
	return si
}

// SyncInfoToProto converts a SyncInfo struct from the hotstuff type to the protobuf type.
func SyncInfoToProto(syncInfo hotstuff.SyncInfo) *SyncInfo {
	m := &SyncInfo{}
	if qc, ok := syncInfo.QC(); ok {
		m.QC = QuorumCertToProto(qc)
	}
	if tc, ok := syncInfo.TC(); ok {
		m.TC = TimeoutCertToProto(tc)
	}
	if aggQC, ok := syncInfo.AggQC(); ok {
		m.AggQC = AggregateQCToProto(aggQC)
	}
	return m
}

// RapidFair: baseline
// 将输入数据转化proto类型的消息
func CollectToProto(col hotstuff.CollectTxSeq) *CollectTxSeq {
	// 获取当前的view ID（baseline中collect阶段的viewID和共识相同）
	// 将交易列表Command转为bytes
	return &CollectTxSeq{
		View:     uint64(col.View()),
		TxSeq:    []byte(col.TxSeq()),
		SyncInfo: SyncInfoToProto(col.SyncInfo()),
	}
}

// 将proto数据转换为hotstuff中可以处理的数据结构
func CollectFromProto(col *CollectTxSeq) hotstuff.CollectTxSeq {
	// command转[]byte到string
	command := hotstuff.Command(col.GetTxSeq())
	syncInfo := SyncInfoFromProto(col.GetSyncInfo())
	return hotstuff.NewCollectTxSeq(hotstuff.View(col.GetView()), command, syncInfo)
}

// 将hotstuff.ReadyCollect数据转为proto类型
func ReadyCollectMsgToProto(rc hotstuff.ReadyCollectMsg) *ReadyCollectMsg {
	return &ReadyCollectMsg{
		View:     uint64(rc.ReadyCollect.View()),
		SyncInfo: SyncInfoToProto(rc.ReadyCollect.SyncInfo()),
	}
}

// 将proto.ReadyCollectMsg数据转为hotstuff.ReadyCollect类型
func ReadyCollectMsgFromProto(rc *ReadyCollectMsg) hotstuff.ReadyCollectMsg {
	syncInfo := SyncInfoFromProto(rc.GetSyncInfo())
	return hotstuff.ReadyCollectMsg{
		ReadyCollect: hotstuff.NewReadyCollect(hotstuff.View(rc.GetView()), syncInfo),
	}
}
