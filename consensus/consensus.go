package consensus

import (
	"fmt"
	"sync"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/consensus/orderfairness"
	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/internal/proto/clientpb"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
	"google.golang.org/protobuf/proto"
)

// Rules is the minimum interface that a consensus implementations must implement.
// Implementations of this interface can be wrapped in the ConsensusBase struct.
// Together, these provide an implementation of the main Consensus interface.
// Implementors do not need to verify certificates or interact with other modules,
// as this is handled by the ConsensusBase struct.
// 所有共识算法: chainedhotstuff,...都实现了Rules接口
type Rules interface {
	// VoteRule decides whether to vote for the block.
	VoteRule(proposal hotstuff.ProposeMsg) bool
	// CommitRule decides whether any ancestor of the block can be committed.
	// Returns the youngest ancestor of the block that can be committed.
	CommitRule(*hotstuff.Block) *hotstuff.Block
	// ChainLength returns the number of blocks that need to be chained together in order to commit.
	ChainLength() int
}

// ProposeRuler is an optional interface that adds a ProposeRule method.
// This allows implementors to specify how new blocks are created.
// ProposeRuler
type ProposeRuler interface {
	// ProposeRule creates a new proposal.
	ProposeRule(cert hotstuff.SyncInfo, cmd hotstuff.Command) (proposal hotstuff.ProposeMsg, ok bool)
}

// consensusBase provides a default implementation of the Consensus interface
// for implementations of the ConsensusImpl interface.
type consensusBase struct {
	impl Rules

	acceptor       modules.Acceptor // Acceptor决定replica是否应该accept a command.
	blockChain     modules.BlockChain
	commandQueue   modules.CommandQueue // commandQueue接口的Get方法只返回string类型？
	configuration  modules.Configuration
	crypto         modules.Crypto
	eventLoop      *eventloop.EventLoop
	executor       modules.ExecutorExt
	forkHandler    modules.ForkHandlerExt
	leaderRotation modules.LeaderRotation
	logger         logging.Logger
	opts           *modules.Options
	synchronizer   modules.Synchronizer
	collector      modules.Collector // RapidFair: baseline引入collect模块

	handel modules.Handel

	lastVote hotstuff.View

	mut   sync.Mutex
	bExec *hotstuff.Block

	marshaler   proto.MarshalOptions   // 序列化（来自proto）
	unmarshaler proto.UnmarshalOptions // 反序列化
}

// New returns a new Consensus instance based on the given Rules implementation.
func New(impl Rules) modules.Consensus {
	return &consensusBase{
		impl:        impl,
		lastVote:    0,
		bExec:       hotstuff.GetGenesis(), // 返回对genesis block的指针
		marshaler:   proto.MarshalOptions{Deterministic: true},
		unmarshaler: proto.UnmarshalOptions{DiscardUnknown: true},
	}
}

// InitModule initializes the module.
// 实现module interface
func (cs *consensusBase) InitModule(mods *modules.Core) {
	mods.Get(
		&cs.acceptor,
		&cs.blockChain,
		&cs.commandQueue,
		&cs.configuration,
		&cs.crypto,
		&cs.eventLoop,
		&cs.executor,
		&cs.forkHandler,
		&cs.leaderRotation,
		&cs.logger,
		&cs.opts,
		&cs.synchronizer,
		&cs.collector, // RapidFair: baseline 获取collector模块
	)

	mods.TryGet(&cs.handel)

	if mod, ok := cs.impl.(modules.Module); ok {
		mod.InitModule(mods)
	}

	cs.eventLoop.RegisterHandler(hotstuff.ProposeMsg{}, func(event any) {
		cs.OnPropose(event.(hotstuff.ProposeMsg))
	})
}

func (cs *consensusBase) CommittedBlock() *hotstuff.Block {
	cs.mut.Lock()
	defer cs.mut.Unlock()
	return cs.bExec
}

// StopVoting ensures that no voting happens in a view earlier than `view`.
// StopVoting保证不会在`view`之前的view中发生投票
func (cs *consensusBase) StopVoting(view hotstuff.View) {
	if cs.lastVote < view {
		cs.lastVote = view
	}
}

// Propose creates a new proposal.
// 这里的consensus算法就只有propose和vote？因为所有hotstuff实现都采用pipelined结构
func (cs *consensusBase) Propose(cert hotstuff.SyncInfo) {
	cs.logger.Debug("Propose")

	// 获取当前highest QC
	qc, ok := cert.QC()
	if ok {
		// tell the acceptor that the previous proposal succeeded.
		if qcBlock, ok := cs.blockChain.Get(qc.BlockHash()); ok {
			// 在leader中更新上一个block中的消息
			cs.acceptor.Proposed(qcBlock.Command())
		} else {
			cs.logger.Errorf("Could not find block for QC: %s", qc)
		}
	}

	// 利用context获取交易输入？ 为什么是从同步器获取proposal的输入？不应该从replica的store获取吗？
	cmd, ok := cs.commandQueue.Get(cs.synchronizer.ViewContext())
	if !ok {
		cs.logger.Debug("Propose: No command")
		return
	}

	v := cs.synchronizer.View()
	cs.logger.Infof("Propose View: %d \n", cs.synchronizer.View())
	if v%100 == 0 {
		cs.logger.Infof("Propose View: %d", cs.synchronizer.View())
	}

	var proposal hotstuff.ProposeMsg
	if proposer, ok := cs.impl.(ProposeRuler); ok {
		proposal, ok = proposer.ProposeRule(cert, cmd)
		if !ok {
			cs.logger.Debug("Propose: No block")
			return
		}
	} else {
		proposal = hotstuff.ProposeMsg{
			ID: cs.opts.ID(),
			Block: hotstuff.NewBlock(
				cs.synchronizer.LeafBlock().Hash(),
				qc,
				cmd,
				cs.synchronizer.View(),
				cs.opts.ID(),
			),
		}

		if aggQC, ok := cert.AggQC(); ok && cs.opts.ShouldUseAggQC() {
			proposal.AggregateQC = &aggQC
		}
	}

	cs.blockChain.Store(proposal.Block)
	// 最后还要调用configuration的Propose方法来发送proposal？网络层面的？
	cs.configuration.Propose(proposal)
	// self vote
	cs.OnPropose(proposal)
}

// replica执行的方法，收到ProposeMsg后节点的处理方式
// RapidFair: 在这里增加verification的过程
func (cs *consensusBase) OnPropose(proposal hotstuff.ProposeMsg) { //nolint:gocyclo
	// TODO: extract parts of this method into helper functions maybe?
	cs.logger.Debugf("OnPropose: %v", proposal.Block)

	block := proposal.Block

	if cs.opts.ShouldUseAggQC() && proposal.AggregateQC != nil {
		highQC, ok := cs.crypto.VerifyAggregateQC(*proposal.AggregateQC)
		if !ok {
			cs.logger.Warn("OnPropose: failed to verify aggregate QC")
			return
		}
		// NOTE: for simplicity, we require that the highQC found in the AggregateQC equals the QC embedded in the block.
		if !block.QuorumCert().Equals(highQC) {
			cs.logger.Warn("OnPropose: block QC does not equal highQC")
			return
		}
	}

	if !cs.crypto.VerifyQuorumCert(block.QuorumCert()) {
		cs.logger.Info("OnPropose: invalid QC")
		return
	}

	// ensure the block came from the leader.
	if proposal.ID != cs.leaderRotation.GetLeader(block.View()) {
		cs.logger.Info("OnPropose: block was not proposed by the expected leader")
		return
	}
	// 判断是否满足vote规则
	if !cs.impl.VoteRule(proposal) {
		cs.logger.Info("OnPropose: Block not voted for")
		return
	}

	// RapidFair: 增加verification流程verifyFairness
	if cs.opts.UseFairOrder() {
		if !cs.VerifyFairness(proposal.Block) {
			cs.logger.Info("OnPropose: Order not verified")
			return
		}
	}

	// 获取给定哈希值的block, Get 将尝试在本地找到该块。如果本地不可用，它将尝试fetch这个block
	// qcBlock指向上一个区块，在OnPropose中replicas确定上一个block的交易已经被提交
	// replica只用写入交易而不用从本地Get交易，但是也要更新已经提交的最高交易序号
	// RapidFair: 公平排序时，需要在collect之前就确定之前区块中已经提交的所有交易
	if !cs.opts.UseFairOrder() {
		if qcBlock, ok := cs.blockChain.Get(block.QuorumCert().BlockHash()); ok {
			// 在replica中更新上一个block已经提交的交易，这之后应该才能执行下一轮collect
			cs.acceptor.Proposed(qcBlock.Command())
		} else {
			cs.logger.Info("OnPropose: Failed to fetch qcBlock")
		}
	}

	// 验证当前提出的block中的tx是否能被接受
	if !cs.acceptor.Accept(block.Command()) {
		cs.logger.Info("OnPropose: command not accepted")
		return
	}

	// 这里就已经确认block可以被接受了？（所以在这之前要先验证）
	// block is safe and was accepted
	cs.blockChain.Store(block)

	didAdvanceView := false
	// we defer the following in order to speed up voting
	defer func() {
		// 具体的CommitRule实现来自./internal/orchestration/worker.go module配置
		if b := cs.impl.CommitRule(block); b != nil {
			cs.commit(b)
		}
		if !didAdvanceView {
			cs.logger.Debugf("[Onpropose]: Replica call advanceView() actively, lastvote view: %d", cs.lastVote)
			cs.synchronizer.AdvanceView(hotstuff.NewSyncInfo().WithQC(block.QuorumCert()))
		}
	}()

	// 如果proposal的block的view小于节点记录的上一个投票的view，则不会继续执行投票
	if block.View() <= cs.lastVote {
		cs.logger.Infof("OnPropose: block view too old, block view: %d, current view: %d, lastVote view: %d", block.View(), cs.synchronizer.View(), cs.lastVote)
		return
	}

	// propose-vote阶段，replica发送给leader的只有PartialCert(pc)，并不包含proposal
	pc, err := cs.crypto.CreatePartialCert(block)
	if err != nil {
		cs.logger.Error("OnPropose: failed to sign block: ", err)
		return
	}

	// cs.lastVote是一个consensus中全局维护的变量，表示上一个投票的view
	// 仅在OnPropose()和stopVoting()中进行修改
	cs.lastVote = block.View()
	// 如果handel不为空，此时会推进view
	if cs.handel != nil {
		// Need to call advanceview such that the view context will be fresh.
		cs.synchronizer.AdvanceView(hotstuff.NewSyncInfo().WithQC(block.QuorumCert()))
		didAdvanceView = true
		cs.handel.Begin(pc)
		return
	}
	// 重点！此时replica将vote发送给下一个view的leader来触发onVote()
	// 这里返回下一个view的leader， cs.lastVote + 1是当前view+1
	leaderID := cs.leaderRotation.GetLeader(cs.lastVote + 1)
	if leaderID == cs.opts.ID() {
		cs.eventLoop.AddEvent(hotstuff.VoteMsg{ID: cs.opts.ID(), PartialCert: pc})
		return
	}

	leader, ok := cs.configuration.Replica(leaderID)
	if !ok {
		cs.logger.Warnf("Replica with ID %d was not found!", leaderID)
		return
	}
	// 远程过程调用？replica远程调用leader对象去vote？
	leader.Vote(pc)
}

func (cs *consensusBase) commit(block *hotstuff.Block) {
	cs.mut.Lock()
	// can't recurse due to requiring the mutex, so we use a helper instead.
	err := cs.commitInner(block)
	cs.mut.Unlock()

	if err != nil {
		cs.logger.Infof("failed to commit: %v", err)
		cs.logger.Warnf("failed to commit: %v", err)
		return
	}

	// prune the blockchain and handle forked blocks
	forkedBlocks := cs.blockChain.PruneToHeight(block.View())
	for _, block := range forkedBlocks {
		cs.logger.Infof("Commit: have forked blocks on view: %d", block.View())
		cs.forkHandler.Fork(block)
	}

	// RapidFair: baseline
	v := block.View()
	if v%100 == 0 { // 每100个block打印一次
		cs.logger.Infof("success to commit block(view): %d", block.View())
	}

}

// recursive helper for commit
func (cs *consensusBase) commitInner(block *hotstuff.Block) error {
	if cs.bExec.View() >= block.View() {
		return nil
	}
	if parent, ok := cs.blockChain.Get(block.Parent()); ok {
		err := cs.commitInner(parent) // 如果当前block有parent，则先递归地commit parent
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to locate block: %s", block.Parent())
	}
	cs.logger.Debug("EXEC: ", block)
	cs.executor.Exec(block) // 在commit之后才真正执行block
	cs.bExec = block
	return nil
}

// ChainLength returns the number of blocks that need to be chained together in order to commit.
func (cs *consensusBase) ChainLength() int {
	return cs.impl.ChainLength()
}

// RapidFair: baseline
// 新增FairPropose()方法，传入公平排序的txSeq参数，创建公平proposal
// 除了获取交易序列的方式以外，其他不改变
func (cs *consensusBase) FairPropose(cert hotstuff.SyncInfo, cmd hotstuff.Command, txSeq map[hotstuff.ID]hotstuff.Command) {
	v := cs.synchronizer.View()
	cs.logger.Infof("FairPropose View: %d, Leader ID: %d\n", v, cs.opts.ID())

	// 获取当前highest QC，通知acceptor之前的proposal已经成功
	qc, ok := cert.QC()
	if ok {
		// tell the acceptor that the previous proposal succeeded.
		if _, ok := cs.blockChain.Get(qc.BlockHash()); !ok {
			// cs.acceptor.Proposed(qcBlock.Command())
			cs.logger.Errorf("Could not find block for QC: %s", qc)
		}
	}

	var proposal hotstuff.ProposeMsg
	if proposer, ok := cs.impl.(ProposeRuler); ok {
		proposal, ok = proposer.ProposeRule(cert, cmd)
		if !ok {
			cs.logger.Debug("Propose: No block")
			return
		}
	} else {
		proposal = hotstuff.ProposeMsg{
			ID: cs.opts.ID(),
			Block: hotstuff.NewFairBlock(
				cs.synchronizer.LeafBlock().Hash(), // 这个应该是HighQC所在的view的block？
				qc,
				cmd,
				cs.synchronizer.View(),
				cs.opts.ID(),
				txSeq,
			),
		}

		if aggQC, ok := cert.AggQC(); ok && cs.opts.ShouldUseAggQC() {
			proposal.AggregateQC = &aggQC
		}
	}

	cs.blockChain.Store(proposal.Block)
	// 最后还要调用configuration的Propose方法来发送proposal？网络层面的？
	cs.configuration.Propose(proposal)
	// self vote
	cs.OnPropose(proposal)
}

// RapidFair: baseline
func (cs *consensusBase) VerifyFairness(proposedBlock *hotstuff.Block) bool {
	isFair := true
	// replica执行公平排序算法验证command是否符合公平排序规则
	proposedTxCmd := proposedBlock.Command()
	unptc := cs.BatchUnmarshal(proposedTxCmd)
	// 对genesis block额外处理，判断command是否为空，为空直接通过
	if proposedBlock.View() == hotstuff.View(1) {
		cs.logger.Infof("is genesis block")
		return isFair
	}
	txSeqList := proposedBlock.TxSeq()
	// 构建txList
	txList, _ := cs.collector.ConstructTxList(txSeqList)
	// 公平排序
	finalTxSeq := orderfairness.FairOrder_Themis(txList, cs.configuration.Len(), cs.opts.ThemisGamma())

	// 判断proposedTx顺序和finalTxSeq顺序是否一致
	if len(unptc) != len(finalTxSeq) { // 先判断下长度是否一致
		isFair = false
		return isFair
	}
	for i, v := range unptc {
		// 计算proposal中的cmd的交易TxID
		txId := hotstuff.NewTxID(v.GetClientID(), v.GetSequenceNumber())
		if string(finalTxSeq[i]) != string(txId) {
			isFair = false
			return isFair
		}
	}
	return isFair
}

// 反序列化 hotstuff.Command -> []byte -> []*clientpb.Command
func (cs *consensusBase) BatchUnmarshal(cmd hotstuff.Command) []*clientpb.Command {
	batch := new(clientpb.Batch)
	err := cs.unmarshaler.Unmarshal([]byte(cmd), batch)
	if err != nil {
		cs.logger.Errorf("[Consensus-VerifyFairness]: Failed to unmarshal batch: %v", err)
		return nil
	}
	return batch.GetCommands()
}
