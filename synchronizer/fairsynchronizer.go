package synchronizer

import (
	"context"
	"fmt"
	"time"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
)

// 实现FairSynchronizer，让replicas对TxSeqFragment同步到同一个virtual view
// 参考hotstuff.Synchronizer
type FairSynchronizer struct {
	tsfChain       modules.TSFChain
	optimisticFO   modules.OptimisticFairOrder
	crypto         modules.Crypto
	configuration  modules.Configuration
	eventLoop      *eventloop.EventLoop
	leaderRotation modules.LeaderRotation
	opts           *modules.Options
	logger         logging.Logger
	commandQueue   modules.CommandQueue
	// RapidFair: 新增virtual view, context, duration, timer等来维护optimisticFairOrder的生命周期（Timeout怎么考虑？）
	curVirView hotstuff.View
	highQC     hotstuff.QuorumCert
	// higtTC     hotstuff.TimeoutCert
	leafTSF *hotstuff.TxSeqFragment

	virViewCtx   context.Context
	cancelVirCtx context.CancelFunc
	virDuration  ViewDuration
	virTimer     *time.Timer

	// 记录最近一次timeout（）
	// lastTimeout *hotstuff.TimeoutMsg

	// map of collected timeout messages per view
	// 记录每一轮每个节点广播的Timeout消息？暂时不用
	timeouts map[hotstuff.View]map[hotstuff.ID]hotstuff.TimeoutMsg
}

func (fs *FairSynchronizer) InitModule(mods *modules.Core) {
	mods.Get(
		&fs.tsfChain,
		&fs.optimisticFO,
		&fs.crypto,
		&fs.configuration,
		&fs.eventLoop,
		&fs.leaderRotation,
		&fs.opts,
		&fs.logger,
		&fs.commandQueue,
	)

	// 注册推进virtual view处理事件
	fs.eventLoop.RegisterHandler(hotstuff.NewVirViewMsg{}, func(event any) {
		fs.OnNewVirView(event.(hotstuff.NewVirViewMsg))
	})

	// 注册超时处理事件（本地）
	fs.eventLoop.RegisterHandler(VirTimeoutEvent{}, func(event any) {
		timeoutView := event.(VirTimeoutEvent).VirView
		if fs.curVirView == timeoutView {
			fs.OnLocalTimeout()
		}
	})

	// 初始化highQC为genesis TxSeqFragment的QC
	var err error
	fs.highQC, err = fs.crypto.CreateQuorumCertTSF(hotstuff.GetGenesisTSF(), []hotstuff.PartialCert{})
	if err != nil {
		panic(fmt.Errorf("unable to create empty quorum cert for genesis TxSeqFragment: %v", err))
	}
}

// 创建一个实现modules.FairSynchronizer的类的对象
func NewFairSync(viewDuration ViewDuration) modules.FairSynchronizer {
	ctx, cancel := context.WithCancel(context.Background())
	return &FairSynchronizer{
		leafTSF:    hotstuff.GetGenesisTSF(),
		curVirView: 1, // 初始的virView就先设置为1

		virViewCtx:   ctx,
		cancelVirCtx: cancel,

		virDuration: viewDuration,
		virTimer:    time.AfterFunc(0, func() {}),

		timeouts: make(map[hotstuff.View]map[hotstuff.ID]hotstuff.TimeoutMsg),
	}
}

// 使用给定的上下文context启动一个virView
func (fs *FairSynchronizer) Start(ctx context.Context) {
	// AfterFunc()返回一个Timer, 在创建后等待s.duration.Duration()的时间，
	// 如果Duration()时间之内没有停止或重置timer，即virView超时，则在自己的 goroutine 中调用func()，这里即是触发timeoutEvent
	// Timer，可用于使用其 Stop 方法取消调用。
	fs.virTimer = time.AfterFunc(fs.virDuration.Duration(), func() {
		fs.cancelVirCtx()
		// 触发VirTimeoutEvent，针对当前的VirView
		fs.eventLoop.AddEvent(VirTimeoutEvent{fs.curVirView})
	})

	go func() {
		<-ctx.Done() // 如果传入的上下文结束了，则就可以结束timer
		fs.virTimer.Stop()
	}()

	if fs.curVirView == 1 { // 启动同步器的时候，所有节点都应该先在cmdCache中写入空fragment
		fs.commandQueue.AddFragment(hotstuff.GetGenesisFragment())
	}
	// 启动之前TSF(0)和Fragment(0)都已经存储过，因此curVirView=1时，让所有raplica运行
	if fs.curVirView == 1 && fs.leaderRotation.GetLeader(fs.curVirView) == fs.opts.ID() {
		// 直接让第一个leader在PreNotify广播genesis TxSeqFragment?
		vsync := hotstuff.NewSyncInfo().WithQC(fs.highQC)
		fs.optimisticFO.PreNotify(vsync)
	}
}

// 返回FairSynchronizer的一部分内部变量
func (fs *FairSynchronizer) HighQC() hotstuff.QuorumCert {
	return fs.highQC
}

// 返回current leaf block
func (fs *FairSynchronizer) LeafTSF() *hotstuff.TxSeqFragment {
	return fs.leafTSF
}

// 返回current virtual view
func (fs *FairSynchronizer) VirView() hotstuff.View {
	return fs.curVirView
}

// 返回current virtual view context
func (fs *FairSynchronizer) VirViewContext() context.Context {
	return fs.virViewCtx
}

// 本地timeout事件，由VirTimeoutEvent触发（先暂时不考虑超时问题）
func (fs *FairSynchronizer) OnLocalTimeout() {
	fs.logger.Debug("[FairSync-OnLocalTimeout]: cause timeout problem")
	// 1. 如果virView的上下文报错(超时)，则重新创建一个virView的上下文
	// if fs.virViewCtx.Err() != nil {
	// 	fs.newVirCtx(fs.virDuration.Duration())
	// }

	// 2. 本地超时的时候，先重置virTimer
	// fs.virTimer.Reset(fs.virDuration.Duration())

	// 3. 判断是否存在lastTimeout；lastTimeout的virView是否和这里触发超时的virView相同

	// 4. 构建timeout消息并广播

	// 4.1 构建TimeoutMsg

	// 赋值lastTimeout
	// 停止对本virView的TSF进行投票？（OptimisticFairOrder控制）

	// 4.2 广播TimeoutMsg
}

// 处理事件NewVirViewMsg的函数
func (fs *FairSynchronizer) OnNewVirView(newVirView hotstuff.NewVirViewMsg) {
	fs.AdvanceVirView(newVirView.VSync)
}

// 推进virtual view的方法
func (fs *FairSynchronizer) AdvanceVirView(vsync hotstuff.SyncInfo) {
	v := hotstuff.View(0)
	// 1. 比较传入的vsync的view和同步器中记录的view的关系
	// 1.1 获取vsync的QC的View（leader构建了QC才会生成vsync，然后再推进virtual view）
	var (
		haveQC bool
		qc     hotstuff.QuorumCert
	)
	if qc, haveQC = vsync.QC(); haveQC { // 验证同步数据里的qc，
		if !fs.crypto.VerifyQuorumCertTSF(qc) {
			fs.logger.Info("Quorum Certificate of TSF could not be verified!")
			return
		}
	}
	if haveQC {
		fs.updateHighQC(qc)
		// if there is both a TC and a QC, we use the QC if its view is greater or equal to the TC.
		if qc.View() >= v {
			v = qc.View()
			// timeout = false
		}
	}
	// 1.2 如果QC的virView < 同步器记录的curVirView，则说明传入的消息是旧消息，无需推进virtual view
	if v < fs.curVirView {
		return
	}

	// 2. 推进同步器的virtual view，推进到QC.VirView+1
	fs.curVirView = v + 1
	// s.lastTimeout = nil
	// 启动一个新view: 将现在的时间记录为新virView的启动时间, viewDuration.startTime = time.Now()
	fs.virDuration.ViewStarted()
	duration := fs.virDuration.Duration()
	fs.newVirCtx(duration)      // 以duration为间隔时间，创建新virView的上下文
	fs.virTimer.Reset(duration) // 以duration的时间间隔重置timer（超时触发方法）
	fs.logger.Debugf("Advanced to virtual view %d", fs.curVirView)

	// virtual leader: 推进view之后才会进行PreNotify
	virleader := fs.leaderRotation.GetLeader(fs.curVirView)
	if virleader == fs.opts.ID() {
		fs.logger.Debugf("[AdvanceVirView]: Leader: QC view v: %d, advance to new curVirView: %d", v, fs.curVirView)
		// 调用OptimisticFairOrder.PreNotify，传入vsync，leader构建PreNotifyMsg广播接收到的(n-f)个交易序列构建的TxSeqFragment
		fs.optimisticFO.PreNotify(vsync)
	}
	// virtual replica: 只推进view，不会发送其他消息
	// fs.logger.Debugf("[AdvanceVirView]: Replica: QC view v: %d, advance to new curVirView: %d", v, fs.curVirView)
}

// 更新本地缓存的最高highQC和leafTSF（TxSeqFragment）
func (fs *FairSynchronizer) updateHighQC(qc hotstuff.QuorumCert) {
	// 1. 确定能找到QC对应的TxSeqFragment
	newTSF, ok := fs.tsfChain.Get(qc.BlockHash())
	if !ok {
		fs.logger.Info("updateHighQC: Could not find TxSeqFragment referenced by new QC!")
		return
	}
	// 2. 查找当前highQC对应的TxSeqFragment
	oldTSF, ok := fs.tsfChain.Get(fs.highQC.BlockHash())
	if !ok {
		fs.logger.Panic("TxSeqFragment from the old highQC missing from chain")
	}
	// 3. 必须newTSF.VirView > oldTSF.VirView 时才会更新highQC 和 leafTSF
	if newTSF.VirView() > oldTSF.VirView() {
		fs.highQC = qc
		fs.leafTSF = newTSF
		fs.logger.Debug("FairSync HighQC updated")
	}
}

// 增加对virtual view，context的管理
func (fs *FairSynchronizer) newVirCtx(duration time.Duration) {
	fs.cancelVirCtx()
	fs.virViewCtx, fs.cancelVirCtx = context.WithTimeout(context.Background(), duration)
}

// 将空指针赋值给变量 _ ,这样可以确保该变量实现了接口类型
var _ modules.FairSynchronizer = (*FairSynchronizer)(nil)

type VirTimeoutEvent struct {
	VirView hotstuff.View
}
