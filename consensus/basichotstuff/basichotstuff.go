// Package chainedhotstuff implements the pipelined three-chain version of the HotStuff protocol.
// [Abandon] basichotstuff对pipeline hotstuff进行修改，不采用pipeline形式，一个round结束之后再开始新round
package orderfairness

import (
	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/consensus"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
)

// 可以使用参数控制执行basichotstuff
func init() {
	modules.RegisterModule("basichotstuff", New)
}

// ChainedHotStuff implements the pipelined three-phase HotStuff protocol.
// 目前BasicHotStuff也是实现了pipelined hotstuff
type BasicHotStuff struct {
	blockChain modules.BlockChain
	logger     logging.Logger

	// protocol variables

	bLock *hotstuff.Block // the currently locked block
}

// New returns a new chainedhotstuff instance.
func New() consensus.Rules {
	return &BasicHotStuff{
		bLock: hotstuff.GetGenesis(),
	}
}

// InitModule initializes the module.
func (hs *BasicHotStuff) InitModule(mods *modules.Core) {
	mods.Get(&hs.blockChain, &hs.logger)
}

func (hs *BasicHotStuff) qcRef(qc hotstuff.QuorumCert) (*hotstuff.Block, bool) {
	if (hotstuff.Hash{}) == qc.BlockHash() {
		return nil, false
	}
	return hs.blockChain.Get(qc.BlockHash())
}

// CommitRule decides whether an ancestor of the block should be committed.
func (hs *BasicHotStuff) CommitRule(block *hotstuff.Block) *hotstuff.Block {
	block1, ok := hs.qcRef(block.QuorumCert())
	if !ok {
		return nil
	}

	// Note that we do not call UpdateHighQC here.
	// This is done through AdvanceView, which the Consensus implementation will call.
	hs.logger.Debug("PRE_COMMIT: ", block1)

	block2, ok := hs.qcRef(block1.QuorumCert())
	if !ok {
		return nil
	}

	if block2.View() > hs.bLock.View() {
		hs.logger.Debug("COMMIT: ", block2)
		hs.bLock = block2
	}

	block3, ok := hs.qcRef(block2.QuorumCert())
	if !ok {
		return nil
	}

	if block1.Parent() == block2.Hash() && block2.Parent() == block3.Hash() {
		hs.logger.Debug("DECIDE: ", block3)
		return block3
	}

	return nil
}

// VoteRule decides whether to vote for the proposal or not.
func (hs *BasicHotStuff) VoteRule(proposal hotstuff.ProposeMsg) bool {
	block := proposal.Block

	qcBlock, haveQCBlock := hs.blockChain.Get(block.QuorumCert().BlockHash())

	safe := false
	if haveQCBlock && qcBlock.View() > hs.bLock.View() {
		safe = true
	} else {
		hs.logger.Debug("OnPropose: liveness condition failed")
		// check if this block extends bLock
		if hs.blockChain.Extends(block, hs.bLock) {
			safe = true
		} else {
			hs.logger.Debug("OnPropose: safety condition failed")
		}
	}

	return safe
}

// ChainLength returns the number of blocks that need to be chained together in order to commit.
func (hs *BasicHotStuff) ChainLength() int {
	return 3
}
