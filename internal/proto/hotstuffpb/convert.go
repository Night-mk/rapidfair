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
	// start := time.Now()
	// elapsed := time.Since(start)
	// fmt.Println("BlockToProto Marshal Execution time:", elapsed)

	// RapidFair: 将FragmentData转为proto格式
	fd := make([]*FragmentData, 0)
	if len(block.FragmentData()) > 0 {
		for _, v := range block.FragmentData() {
			fd = append(fd, FragmentDataToProto(&v))
		}
	}
	return &Block{
		Parent:       parentHash[:],
		Command:      []byte(block.Command()),
		QC:           QuorumCertToProto(block.QuorumCert()),
		View:         uint64(block.View()),
		Proposer:     uint32(block.Proposer()),
		TxSeq:        txSeqB, // 增加TxSeq字段
		FragmentData: fd,
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
	// start := time.Now()
	// elapsed := time.Since(start)
	// fmt.Println("BlockFromProto Unmarshal Execution time:", elapsed)

	// RapidFair 使用NewRapidFairBlock()构造block
	fragDataProto := block.GetFragmentData()
	if len(fragDataProto) > 0 {
		// 将proto转为hotstuff.FragmentData类型
		fragData := make([]hotstuff.FragmentData, 0)
		for _, v := range fragDataProto {
			fd := FragmentDataFromProto(v)
			fragData = append(fragData, *fd)
		}
		return hotstuff.NewRapidFairBlock(
			p,
			QuorumCertFromProto(block.GetQC()),
			hotstuff.Command(block.GetCommand()),
			hotstuff.View(block.GetView()),
			hotstuff.ID(block.GetProposer()),
			fragData,
		)
	}

	if len(txSeq) > 0 { // Themis block
		return hotstuff.NewFairBlock(
			p,
			QuorumCertFromProto(block.GetQC()),
			hotstuff.Command(block.GetCommand()),
			hotstuff.View(block.GetView()),
			hotstuff.ID(block.GetProposer()),
			txSeq,
		)
	} else { // hotstuff block
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

// 将hotstuff.MultiCollect转为proto类型
func MultiCollectToProto(mc hotstuff.MultiCollectMsg) *MCollect {
	return &MCollect{
		VirView: uint64(mc.MultiCollect.VirView()),
		TxSeq:   []byte(mc.MultiCollect.TxSeq()),
		PC:      PartialCertToProto(mc.MultiCollect.PartialCert()),
	}
}

// 将proto转为hotstuff.MultiCollect类型
func MultiCollectFromProto(mc *MCollect) hotstuff.MultiCollectMsg {
	txSeq := hotstuff.Command(mc.GetTxSeq())
	pc := PartialCertFromProto(mc.GetPC())
	// return hotstuff.NewMultiCollect(hotstuff.View(mc.GetVirView()), txSeq, pc)
	return hotstuff.MultiCollectMsg{
		MultiCollect: hotstuff.NewMultiCollect(hotstuff.View(mc.GetVirView()), txSeq, pc),
	}
}

// 将hotstuff.PreNotify转为proto类型
func PreNotifyToProto(pn hotstuff.PreNotifyMsg) *PreNotifyMsg {
	parentHash := pn.TxSeqFragment.Parent()
	maphash := make(map[uint32][]byte)
	if len(pn.TxSeqFragment.TxSeqHash()) > 0 {
		for k, v := range pn.TxSeqFragment.TxSeqHash() {
			maphash[uint32(k)] = []byte(v)
		}
	}

	tsf := &TxSeqFragment{
		Parent:    parentHash[:],
		QC:        QuorumCertToProto(pn.TxSeqFragment.QuorumCert()),
		VirView:   uint64(pn.TxSeqFragment.VirView()),
		Leader:    uint32(pn.TxSeqFragment.VirLeader()),
		TxSeqHash: maphash,
	}

	return &PreNotifyMsg{
		TxSeqFragment: tsf,
	}
}

// 将proto转为hotstuff.PreNotify类型
func PreNotifyFromProto(pn *PreNotifyMsg) hotstuff.PreNotifyMsg {
	tsh := make(hotstuff.TXList)
	if len(pn.TxSeqFragment.GetTxSeqHash()) > 0 {
		for k, v := range pn.TxSeqFragment.GetTxSeqHash() {
			// var h hotstuff.Hash
			// copy(h[:], v)
			tsh[hotstuff.ID(k)] = hotstuff.Command(v)
		}
	}

	tsf := pn.GetTxSeqFragment()
	var parentHash hotstuff.Hash
	copy(parentHash[:], tsf.GetParent())

	return hotstuff.PreNotifyMsg{
		TxSeqFragment: hotstuff.NewTxSeqFragment(
			parentHash,
			QuorumCertFromProto(tsf.GetQC()),
			hotstuff.View(tsf.GetVirView()),
			hotstuff.ID(tsf.GetLeader()),
			tsh,
		),
	}
}

// RapidFair: 将hotstuff.FragmentData转为proto类型
func FragmentDataToProto(fd *hotstuff.FragmentData) *FragmentData {
	hash := fd.Hash()
	txSeqHash := fd.TxSeq()
	updateSeqHash := fd.UpdateSeq()

	return &FragmentData{
		Hash:      hash[:],
		VirView:   uint64(fd.VirView()),
		OrderedTx: []byte(fd.OrderedTx()),
		TxSeq:     txSeqHash[:],
		MissEdge:  []byte(fd.MissEdge()),
		UpdateSeq: updateSeqHash[:],
	}
}

// 将proto转为hotstuff.FragmentData类型
func FragmentDataFromProto(fd *FragmentData) *hotstuff.FragmentData {
	var (
		txSeqHash     hotstuff.Hash
		updateSeqHash hotstuff.Hash
	)
	copy(txSeqHash[:], fd.GetTxSeq())
	copy(updateSeqHash[:], fd.GetUpdateSeq())

	return hotstuff.NewFragmentData(
		hotstuff.View(fd.GetVirView()),
		hotstuff.Command(fd.GetOrderedTx()),
		txSeqHash,
		hotstuff.Command(fd.GetMissEdge()),
		updateSeqHash,
	)
}
