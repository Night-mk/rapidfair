package consensus

import (
	"sync"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
)

// VotingMachine collects votes.
type VotingMachine struct {
	blockChain    modules.BlockChain
	configuration modules.Configuration
	crypto        modules.Crypto
	eventLoop     *eventloop.EventLoop
	logger        logging.Logger
	synchronizer  modules.Synchronizer
	opts          *modules.Options

	mut           sync.Mutex
	verifiedVotes map[hotstuff.Hash][]hotstuff.PartialCert // verified votes that could become a QC
}

// NewVotingMachine returns a new VotingMachine.
func NewVotingMachine() *VotingMachine {
	return &VotingMachine{
		verifiedVotes: make(map[hotstuff.Hash][]hotstuff.PartialCert),
	}
}

// InitModule initializes the VotingMachine.
func (vm *VotingMachine) InitModule(mods *modules.Core) {
	mods.Get(
		&vm.blockChain,
		&vm.configuration,
		&vm.crypto,
		&vm.eventLoop,
		&vm.logger,
		&vm.synchronizer,
		&vm.opts,
	)

	vm.eventLoop.RegisterHandler(hotstuff.VoteMsg{}, func(event any) { vm.OnVote(event.(hotstuff.VoteMsg)) })
}

// OnVote handles an incoming vote. 应该只有当前View的Leader节点来处理
func (vm *VotingMachine) OnVote(vote hotstuff.VoteMsg) {
	cert := vote.PartialCert
	vm.logger.Debugf("OnVote(%d): %.8s", vote.ID, cert.BlockHash())

	var (
		block *hotstuff.Block
		ok    bool
	)

	if !vote.Deferred {
		// first, try to get the block from the local cache
		block, ok = vm.blockChain.LocalGet(cert.BlockHash())
		if !ok {
			// if that does not work, we will try to handle this event later.
			// hopefully, the block has arrived by then.
			vm.logger.Debugf("Local cache miss for block: %.8s", cert.BlockHash())
			vote.Deferred = true
			vm.eventLoop.DelayUntil(hotstuff.ProposeMsg{}, vote)
			return
		}
	} else {
		// if the block has not arrived at this point we will try to fetch it.
		block, ok = vm.blockChain.Get(cert.BlockHash())
		if !ok {
			vm.logger.Debugf("Could not find block for vote: %.8s.", cert.BlockHash())
			return
		}
	}

	if block.View() <= vm.synchronizer.LeafBlock().View() {
		// too old
		return
	}

	if vm.opts.ShouldVerifyVotesSync() {
		vm.verifyCert(cert, block)
	} else {
		go vm.verifyCert(cert, block) // 启动一个新的routine运行函数
	}
}

func (vm *VotingMachine) verifyCert(cert hotstuff.PartialCert, block *hotstuff.Block) {
	if !vm.crypto.VerifyPartialCert(cert) {
		vm.logger.Info("OnVote: Vote could not be verified!")
		return
	}

	vm.mut.Lock()
	defer vm.mut.Unlock()

	// this defer will clean up any old votes in verifiedVotes
	defer func() {
		// delete any pending QCs with lower height than bLeaf
		for k := range vm.verifiedVotes {
			if block, ok := vm.blockChain.LocalGet(k); ok {
				if block.View() <= vm.synchronizer.LeafBlock().View() {
					delete(vm.verifiedVotes, k)
				}
			} else {
				delete(vm.verifiedVotes, k)
			}
		}
	}()

	votes := vm.verifiedVotes[cert.BlockHash()]
	votes = append(votes, cert)
	vm.verifiedVotes[cert.BlockHash()] = votes
	// 如果vote数量小于quorum时，就不做其他处理
	if len(votes) < vm.configuration.QuorumSize() {
		return
	}
	// vote数量超过quorum，则创建QC
	qc, err := vm.crypto.CreateQuorumCert(block, votes)
	if err != nil {
		vm.logger.Info("OnVote: could not create QC for block: ", err)
		return
	}
	delete(vm.verifiedVotes, cert.BlockHash())
	// Leader每次在接收到足够多vote最后都发送NewViewMsg来推进view（默认采用pipelined结构hotstuff？）
	// 处理NewViewMsg的方法在synchronizer.go中，使用AdvanceView()
	vm.logger.Infof("[OnVote]: Leader call advanceView() actively")
	vm.eventLoop.AddEvent(hotstuff.NewViewMsg{ID: vm.opts.ID(), SyncInfo: hotstuff.NewSyncInfo().WithQC(qc)})
}
