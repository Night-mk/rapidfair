package modules

import (
	"context"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/internal/proto/clientpb"
)

// Module interfaces

//go:generate mockgen -destination=../internal/mocks/cmdqueue_mock.go -package=mocks . CommandQueue

// CommandQueue is a queue of commands to be proposed.
type CommandQueue interface {
	// Get returns the next command to be proposed. Get返回下一个需要propose的command
	// It may run until the context is cancelled.
	// If no command is available, the 'ok' return value should be false.
	Get(ctx context.Context) (cmd hotstuff.Command, ok bool)

	// RapidFair: baseline GetTxBatch让replica返回下一个希望提交的交易序列
	GetTxBatch(ctx context.Context) (cmd hotstuff.Command, ok bool)

	// RapidFair
	// optimisticfairorder将Fragment写入blockchain
	AddFragment(*hotstuff.Fragment)
	// 让consensus的replica返回已经缓存的Fragment
	GetFragment(ctx context.Context) (fragments []*hotstuff.Fragment, ok bool)
}

//go:generate mockgen -destination=../internal/mocks/acceptor_mock.go -package=mocks . Acceptor

// Acceptor decides if a replica should accept a command.
type Acceptor interface {
	// Accept returns true if the replica should accept the command, false otherwise.
	Accept(hotstuff.Command) bool
	// Proposed tells the acceptor that the propose phase for the given command succeeded, and it should no longer be
	// accepted in the future.
	Proposed(hotstuff.Command)

	// RapidFair 为acceptor提供额外接口方法
	// 使用ProposedFragment确定哪些交易已经提交到Fragment
	ProposedFragment(hotstuff.Command)
	// 在RapidFair中，共识流程提交Block的交易时，采用这个方法
	ProposedBlock(hotstuff.Command)
}

//go:generate mockgen -destination=../internal/mocks/executor_mock.go -package=mocks . Executor

// Executor is responsible for executing the commands that are committed by the consensus protocol.
type Executor interface {
	// Exec executes the command.
	Exec(cmd hotstuff.Command)
}

// ExecutorExt is responsible for executing the commands that are committed by the consensus protocol.
//
// This interface is similar to the Executor interface, except it takes a block as an argument, instead of a command,
// making it more flexible than the alternative interface.
type ExecutorExt interface {
	// Exec executes the command in the block.
	Exec(block *hotstuff.Block)
}

//go:generate mockgen -destination=../internal/mocks/forkhandler_mock.go -package=mocks . ForkHandler

// ForkHandler handles commands that do not get committed due to a forked blockchain.
//
// TODO: think of a better name/interface
type ForkHandler interface {
	// Fork handles the command from a forked block.
	Fork(cmd hotstuff.Command)
}

// ForkHandlerExt handles blocks that do not get committed due to a fork of the blockchain.
//
// This interface is similar to the ForkHandler interface, except it takes a block as an argument, instead of a command.
type ForkHandlerExt interface {
	// Fork handles the forked block.
	Fork(block *hotstuff.Block)
}

// BlockChain is a datastructure that stores a chain of blocks.
// It is not required that a block is stored forever,
// but a block must be stored until at least one of its children have been committed.
type BlockChain interface {
	// Store stores a block in the blockchain.
	Store(*hotstuff.Block)

	// Get retrieves a block given its hash, attempting to fetching it from other replicas if necessary.
	Get(hotstuff.Hash) (*hotstuff.Block, bool)

	// LocalGet retrieves a block given its hash, without fetching it from other replicas.
	LocalGet(hotstuff.Hash) (*hotstuff.Block, bool)

	// Extends checks if the given block extends the branch of the target hash.
	Extends(block, target *hotstuff.Block) bool

	// Prunes blocks from the in-memory tree up to the specified height.
	// Returns a set of forked blocks (blocks that were on a different branch, and thus not committed).
	PruneToHeight(height hotstuff.View) (forkedBlocks []*hotstuff.Block)
}

//go:generate mockgen -destination=../internal/mocks/replica_mock.go -package=mocks . Replica

// Replica represents a remote replica participating in the consensus protocol.
// The methods Vote, NewView, and Deliver must send the respective arguments to the remote replica.
type Replica interface {
	// ID returns the replica's id.
	ID() hotstuff.ID
	// PublicKey returns the replica's public key.
	PublicKey() hotstuff.PublicKey
	// Vote sends the partial certificate to the other replica.
	Vote(cert hotstuff.PartialCert)
	// NewView sends the quorum certificate to the other replica.
	NewView(hotstuff.SyncInfo)
	// Metadata returns the connection metadata sent by this replica.
	Metadata() map[string]string
	// RapidFair: baseline接口中增加Collect方法，在./backend/config.go中实现
	Collect(hotstuff.CollectTxSeq)

	// RapidFair: 增加MultiCollect，replica发送MultiCollect消息给Leader
	MultiCollect(hotstuff.MultiCollectMsg)
}

//go:generate mockgen -destination=../internal/mocks/configuration_mock.go -package=mocks . Configuration

// Configuration holds information about the current configuration of replicas that participate in the protocol,
// It provides methods to send messages to the other replicas.
type Configuration interface {
	// Replicas returns all of the replicas in the configuration.
	Replicas() map[hotstuff.ID]Replica
	// Replica returns a replica if present in the configuration.
	Replica(hotstuff.ID) (replica Replica, ok bool)
	// Len returns the number of replicas in the configuration.
	Len() int
	// QuorumSize returns the size of a quorum.
	QuorumSize() int
	// Propose sends the block to all replicas in the configuration.
	Propose(proposal hotstuff.ProposeMsg)
	// Timeout sends the timeout message to all replicas.
	Timeout(msg hotstuff.TimeoutMsg)
	// Fetch requests a block from all the replicas in the configuration.
	Fetch(ctx context.Context, hash hotstuff.Hash) (block *hotstuff.Block, ok bool)
	// SubConfig returns a subconfiguration containing the replicas specified in the ids slice.
	SubConfig(ids []hotstuff.ID) (sub Configuration, err error)

	// RapidFair: baseline接口中增加ReadyCollect方法，在./backend/config.go中实现
	ReadyCollect(hotstuff.ReadyCollectMsg)
	// RapidFair: 返回公平排序定义下的quorumsize
	QuorumSizeFair() int
	// RapidFair: 增加PreNotify，leader广播PreNotify消息给所有replicas
	PreNotify(hotstuff.PreNotifyMsg)
}

//go:generate mockgen -destination=../internal/mocks/consensus_mock.go -package=mocks . Consensus

// Consensus implements a byzantine consensus protocol, such as HotStuff.
// It contains the protocol data for a single replica.
// The methods OnPropose, OnVote, OnNewView, and OnDeliver should be called upon receiving a corresponding message.
type Consensus interface {
	// StopVoting ensures that no voting happens in a view earlier than `view`.
	StopVoting(view hotstuff.View)
	// Propose starts a new proposal. The command is fetched from the command queue.
	Propose(cert hotstuff.SyncInfo)
	// CommittedBlock returns the most recently committed block.
	CommittedBlock() *hotstuff.Block
	// ChainLength returns the number of blocks that need to be chained together in order to commit.
	ChainLength() int
	// RapiFair: baseline FairPropose输入公平排序数据用于提出proposal
	FairPropose(cert hotstuff.SyncInfo, cmd hotstuff.Command, txSeq map[hotstuff.ID]hotstuff.Command)
	// RapidFair: 获取Fragments构建block用于提出block
	RapidFairPropose(cert hotstuff.SyncInfo)
}

// LeaderRotation implements a leader rotation scheme.
type LeaderRotation interface {
	// GetLeader returns the id of the leader in the given view.
	GetLeader(hotstuff.View) hotstuff.ID
}

//go:generate mockgen -destination=../internal/mocks/synchronizer_mock.go -package=mocks . Synchronizer

// Synchronizer synchronizes replicas to the same view.
type Synchronizer interface {
	// AdvanceView attempts to advance to the next view using the given QC.
	// qc must be either a regular quorum certificate, or a timeout certificate.
	AdvanceView(hotstuff.SyncInfo)
	// View returns the current view.
	View() hotstuff.View
	// ViewContext returns a context that is cancelled at the end of the view.
	ViewContext() context.Context
	// HighQC returns the highest known QC.
	HighQC() hotstuff.QuorumCert
	// LeafBlock returns the current leaf block.
	LeafBlock() *hotstuff.Block
	// Start starts the synchronizer with the given context.
	Start(context.Context)

	// RapidFair: 增加对virtual view的维护
	// VirView() hotstuff.View
	// VirViewContext() context.Context
}

// Handel is an implementation of the Handel signature aggregation protocol.
type Handel interface {
	// Begin commissions the aggregation of a new signature.
	Begin(s hotstuff.PartialCert)
}

// ExtendedExecutor turns the given Executor into an ExecutorExt.
func ExtendedExecutor(executor Executor) ExecutorExt {
	return executorWrapper{executor}
}

type executorWrapper struct {
	executor Executor
}

func (ew executorWrapper) InitModule(mods *Core) {
	if m, ok := ew.executor.(Module); ok {
		m.InitModule(mods)
	}
}

func (ew executorWrapper) Exec(block *hotstuff.Block) {
	ew.executor.Exec(block.Command())
}

// ExtendedForkHandler turns the given ForkHandler into a ForkHandlerExt.
func ExtendedForkHandler(forkHandler ForkHandler) ForkHandlerExt {
	return forkHandlerWrapper{forkHandler}
}

type forkHandlerWrapper struct {
	forkHandler ForkHandler
}

func (fhw forkHandlerWrapper) InitModule(mods *Core) {
	if m, ok := fhw.forkHandler.(Module); ok {
		m.InitModule(mods)
	}
}

func (fhw forkHandlerWrapper) Fork(block *hotstuff.Block) {
	fhw.forkHandler.Fork(block.Command())
}

// RapidFair: baseline 增加collect模块
// Collector实现交易序列收集协议
type Collector interface {
	// replica发送交易序列给leader
	Collect(syncInfo hotstuff.SyncInfo)
	// 公开构建txList的方法
	// ConstructTxList(txSeq map[hotstuff.ID]hotstuff.Command) ([][]string, map[string]*clientpb.Command)
	ConstructTxList(txSeq hotstuff.TXList) ([][]string, map[string]*clientpb.Command)
	// leader广播能进行collect的消息给所有replicas
	ReadyCollect(syncInfo hotstuff.SyncInfo)
	// 公平排序将排序结果序列化为Command返回
	FairOrder(cTxSeq hotstuff.TXList) hotstuff.Command
}

// RapidFair: 增加FragmentChain控制多个fragment(类似blockchain)
type FragmentChain interface {
	// 存储提出的fragment到FragmentChain中
	Store(*hotstuff.Fragment)

	LocalGet(hotstuff.Hash) (*hotstuff.Fragment, bool)

	Get(hotstuff.Hash) (*hotstuff.Fragment, bool)

	LocalGetAtHeight(hotstuff.View) (*hotstuff.Fragment, bool)
}

type TSFChain interface {
	// 存储提出的fragment到TSFragmentChain中
	Store(*hotstuff.TxSeqFragment)

	LocalGet(hotstuff.Hash) (*hotstuff.TxSeqFragment, bool)

	Get(hotstuff.Hash) (*hotstuff.TxSeqFragment, bool)
}

// RapidFair: 增加optimisticfairorder接口
type OptimisticFairOrder interface {
	// virtual leader使用PreNotify提出PreNotifyMsg{ID, TxSeqFragment}
	PreNotify(vsync hotstuff.SyncInfo)
}

// 增加FairSynchronizer接口作为TxSeqFragment的同步器
// 具体方法很接近Synchronizer
type FairSynchronizer interface {
	// 推进virtual view
	AdvanceVirView(hotstuff.SyncInfo)
	// 返回当前的virtual view
	VirView() hotstuff.View
	// 返回当前的virtual view对应的上下文
	VirViewContext() context.Context
	// 返回当前本地同步器中记录的最新TxSeqFragment
	LeafTSF() *hotstuff.TxSeqFragment
	// 使用给定的上下文启动同步器
	Start(context.Context)
}
