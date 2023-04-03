// Package crypto provides implementations of the Crypto interface.
package crypto

import (
	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/modules"
)

type crypto struct {
	blockChain    modules.BlockChain
	configuration modules.Configuration
	tsfChain      modules.TSFChain // RapidChain 增加TxSeqFragmentChain

	modules.CryptoBase
}

// New returns a new implementation of the Crypto interface. It will use the given CryptoBase to create and verify
// signatures.
func New(impl modules.CryptoBase) modules.Crypto {
	return &crypto{CryptoBase: impl}
}

// InitModule gives the module a reference to the Core object.
// It also allows the module to set module options using the OptionsBuilder.
func (c *crypto) InitModule(mods *modules.Core) {
	mods.Get(
		&c.blockChain,
		&c.configuration,
		&c.tsfChain,
	)

	if mod, ok := c.CryptoBase.(modules.Module); ok {
		mod.InitModule(mods)
	}
}

// CreatePartialCert signs a single block and returns the partial certificate.
func (c crypto) CreatePartialCert(block *hotstuff.Block) (cert hotstuff.PartialCert, err error) {
	sig, err := c.Sign(block.ToBytes())
	if err != nil {
		return hotstuff.PartialCert{}, err
	}
	return hotstuff.NewPartialCert(sig, block.Hash()), nil
}

// CreateQuorumCert creates a quorum certificate from a list of partial certificates.
func (c crypto) CreateQuorumCert(block *hotstuff.Block, signatures []hotstuff.PartialCert) (cert hotstuff.QuorumCert, err error) {
	// genesis QC is always valid.
	if block.Hash() == hotstuff.GetGenesis().Hash() {
		return hotstuff.NewQuorumCert(nil, 0, hotstuff.GetGenesis().Hash()), nil
	}
	sigs := make([]hotstuff.QuorumSignature, 0, len(signatures))
	for _, sig := range signatures {
		sigs = append(sigs, sig.Signature())
	}
	sig, err := c.Combine(sigs...)
	if err != nil {
		return hotstuff.QuorumCert{}, err
	}
	return hotstuff.NewQuorumCert(sig, block.View(), block.Hash()), nil
}

// CreateTimeoutCert creates a timeout certificate from a list of timeout messages.
func (c crypto) CreateTimeoutCert(view hotstuff.View, timeouts []hotstuff.TimeoutMsg) (cert hotstuff.TimeoutCert, err error) {
	// view 0 is always valid.
	if view == 0 {
		return hotstuff.NewTimeoutCert(nil, 0), nil
	}
	sigs := make([]hotstuff.QuorumSignature, 0, len(timeouts))
	for _, timeout := range timeouts {
		sigs = append(sigs, timeout.ViewSignature)
	}
	sig, err := c.Combine(sigs...)
	if err != nil {
		return hotstuff.TimeoutCert{}, err
	}
	return hotstuff.NewTimeoutCert(sig, view), nil
}

// CreateAggregateQC creates an AggregateQC from the given timeout messages.
func (c crypto) CreateAggregateQC(view hotstuff.View, timeouts []hotstuff.TimeoutMsg) (aggQC hotstuff.AggregateQC, err error) {
	qcs := make(map[hotstuff.ID]hotstuff.QuorumCert)
	sigs := make([]hotstuff.QuorumSignature, 0, len(timeouts))
	for _, timeout := range timeouts {
		if qc, ok := timeout.SyncInfo.QC(); ok {
			qcs[timeout.ID] = qc
		}
		if timeout.MsgSignature != nil {
			sigs = append(sigs, timeout.MsgSignature)
		}
	}
	sig, err := c.Combine(sigs...)
	if err != nil {
		return hotstuff.AggregateQC{}, err
	}
	return hotstuff.NewAggregateQC(qcs, sig, view), nil
}

// VerifyPartialCert verifies a single partial certificate.
func (c crypto) VerifyPartialCert(cert hotstuff.PartialCert) bool {
	// fmt.Println("VerifyPartialCert======================")
	block, ok := c.blockChain.Get(cert.BlockHash())
	if !ok {
		return false
	}
	return c.Verify(cert.Signature(), block.ToBytes())
}

// VerifyQuorumCert verifies a quorum certificate.
func (c crypto) VerifyQuorumCert(qc hotstuff.QuorumCert) bool {
	// genesis QC is always valid.
	if qc.BlockHash() == hotstuff.GetGenesis().Hash() {
		return true
	}
	if qc.Signature().Participants().Len() < c.configuration.QuorumSize() {
		return false
	}
	block, ok := c.blockChain.Get(qc.BlockHash())
	if !ok {
		return false
	}
	return c.Verify(qc.Signature(), block.ToBytes())
}

// VerifyTimeoutCert verifies a timeout certificate.
func (c crypto) VerifyTimeoutCert(tc hotstuff.TimeoutCert) bool {
	// view 0 TC is always valid.
	if tc.View() == 0 {
		return true
	}
	if tc.Signature().Participants().Len() < c.configuration.QuorumSize() {
		return false
	}
	return c.Verify(tc.Signature(), tc.View().ToBytes())
}

// VerifyAggregateQC verifies the AggregateQC and returns the highQC, if valid.
func (c crypto) VerifyAggregateQC(aggQC hotstuff.AggregateQC) (highQC hotstuff.QuorumCert, ok bool) {
	messages := make(map[hotstuff.ID][]byte)
	for id, qc := range aggQC.QCs() {
		if highQC.View() < qc.View() || highQC == (hotstuff.QuorumCert{}) {
			highQC = qc
		}
		// reconstruct the TimeoutMsg to get the hash
		messages[id] = hotstuff.TimeoutMsg{
			ID:       id,
			View:     aggQC.View(),
			SyncInfo: hotstuff.NewSyncInfo().WithQC(qc),
		}.ToBytes()
	}
	if aggQC.Sig().Participants().Len() < c.configuration.QuorumSize() {
		return hotstuff.QuorumCert{}, false
	}
	// both the batched aggQC signatures and the highQC must be verified
	if c.BatchVerify(aggQC.Sig(), messages) && c.VerifyQuorumCert(highQC) {
		return highQC, true
	}
	return hotstuff.QuorumCert{}, false
}

// RapidChain: 实现对TxSeqFragment的阈值签名相关方法
// CreatePartialCertTSF signs a single TxSeqFragment and returns the partial certificate.
func (c crypto) CreatePartialCertTSF(tsf *hotstuff.TxSeqFragment) (cert hotstuff.PartialCert, err error) {
	sig, err := c.Sign(tsf.ToBytes())
	if err != nil {
		return hotstuff.PartialCert{}, err
	}
	return hotstuff.NewPartialCert(sig, tsf.Hash()), nil
}

// 创建TxSeqFragment的聚合签名
// CreateQuorumCertTSF creates a quorum certificate from a list of partial certificates (TxSeqFragment).
func (c crypto) CreateQuorumCertTSF(tsf *hotstuff.TxSeqFragment, signatures []hotstuff.PartialCert) (cert hotstuff.QuorumCert, err error) {
	// genesis QC is always valid.
	if tsf.Hash() == hotstuff.GetGenesisTSF().Hash() {
		return hotstuff.NewQuorumCert(nil, 0, hotstuff.GetGenesisTSF().Hash()), nil
	}
	sigs := make([]hotstuff.QuorumSignature, 0, len(signatures))
	for _, sig := range signatures {
		sigs = append(sigs, sig.Signature())
	}
	sig, err := c.Combine(sigs...)
	if err != nil {
		return hotstuff.QuorumCert{}, err
	}
	return hotstuff.NewQuorumCert(sig, tsf.VirView(), tsf.Hash()), nil
}

// 验证TxSeqFragment部分签名
// VerifyPartialCertTSF verifies a single partial certificate (TxSeqFragment).
func (c crypto) VerifyPartialCertTSF(cert hotstuff.PartialCert) bool {
	tsf, ok := c.tsfChain.Get(cert.BlockHash()) // 这里cert.BlockHash()就是指的TxSeqFragment的hash值
	if !ok {
		return false
	}
	return c.Verify(cert.Signature(), tsf.ToBytes())
}

// 验证TxSeqFragment聚合签名
// VerifyPartialCertTSF verifies a quorum certificate for (TxSeqFragment).
func (c crypto) VerifyQuorumCertTSF(qc hotstuff.QuorumCert) bool {
	// genesis QC is always valid.
	if qc.BlockHash() == hotstuff.GetGenesisTSF().Hash() {
		return true
	}
	// 这里要对比QuorumSizeFair()，要达到公平排序的恶意节点需求？
	if qc.Signature().Participants().Len() < c.configuration.QuorumSizeFair() {
		return false
	}
	tsf, ok := c.tsfChain.Get(qc.BlockHash())
	if !ok {
		return false
	}
	return c.Verify(qc.Signature(), tsf.ToBytes())
}
