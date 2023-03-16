package synchronizer

import (
	"context"
	"fmt"
	"time"

	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"

	"github.com/relab/hotstuff"
)

// Synchronizer synchronizes replicas to the same view.
// Synchronizer将replicas同步到同一个view
type Synchronizer struct {
	blockChain     modules.BlockChain
	consensus      modules.Consensus
	crypto         modules.Crypto
	configuration  modules.Configuration
	eventLoop      *eventloop.EventLoop
	leaderRotation modules.LeaderRotation
	logger         logging.Logger
	opts           *modules.Options
	col            modules.Collector // RapidFair: baseline 增加调用collect的模块

	currentView hotstuff.View
	highTC      hotstuff.TimeoutCert
	highQC      hotstuff.QuorumCert
	leafBlock   *hotstuff.Block

	// A pointer to the last timeout message that we sent.
	// If a timeout happens again before we advance to the next view,
	// we will simply send this timeout again.
	lastTimeout *hotstuff.TimeoutMsg

	duration ViewDuration
	timer    *time.Timer

	viewCtx   context.Context // a context that is cancelled at the end of the current view
	cancelCtx context.CancelFunc

	// map of collected timeout messages per view
	timeouts map[hotstuff.View]map[hotstuff.ID]hotstuff.TimeoutMsg
}

// InitModule initializes the synchronizer.
func (s *Synchronizer) InitModule(mods *modules.Core) {
	mods.Get(
		&s.blockChain,
		&s.consensus,
		&s.crypto,
		&s.configuration,
		&s.eventLoop,
		&s.leaderRotation,
		&s.logger,
		&s.opts,
		&s.col,
	)

	s.eventLoop.RegisterHandler(TimeoutEvent{}, func(event any) {
		timeoutView := event.(TimeoutEvent).View
		if s.currentView == timeoutView {
			s.OnLocalTimeout()
		}
	})

	s.eventLoop.RegisterHandler(hotstuff.NewViewMsg{}, func(event any) {
		newViewMsg := event.(hotstuff.NewViewMsg)
		s.OnNewView(newViewMsg)
	})

	s.eventLoop.RegisterHandler(hotstuff.TimeoutMsg{}, func(event any) {
		timeoutMsg := event.(hotstuff.TimeoutMsg)
		s.OnRemoteTimeout(timeoutMsg)
	})

	var err error
	// 初始化的时候highQC从genesis获取
	s.highQC, err = s.crypto.CreateQuorumCert(hotstuff.GetGenesis(), []hotstuff.PartialCert{})
	if err != nil {
		panic(fmt.Errorf("unable to create empty quorum cert for genesis block: %v", err))
	}
	s.highTC, err = s.crypto.CreateTimeoutCert(hotstuff.View(0), []hotstuff.TimeoutMsg{})
	if err != nil {
		panic(fmt.Errorf("unable to create empty timeout cert for view 0: %v", err))
	}
}

// New creates a new Synchronizer.
func New(viewDuration ViewDuration) modules.Synchronizer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Synchronizer{
		leafBlock:   hotstuff.GetGenesis(),
		currentView: 1, // 初始化的时候将currentView设置为了1

		viewCtx:   ctx, // 创建空上下文
		cancelCtx: cancel,

		duration: viewDuration,
		timer:    time.AfterFunc(0, func() {}), // dummy timer that will be replaced after start() is called

		timeouts: make(map[hotstuff.View]map[hotstuff.ID]hotstuff.TimeoutMsg),
	}
}

// Start starts the synchronizer with the given context.
// 在rapidfair/replica/replica.go的Run()方法中被调用
func (s *Synchronizer) Start(ctx context.Context) {
	// 在replica中启动后等待s.duration.Duration()的时间，
	// AfterFunc() 等待持续时间Duration()过去，然后在自己的 goroutine 中调用func()
	// AfterFunc()它返回一个 Timer，可用于使用其 Stop 方法取消调用。
	s.timer = time.AfterFunc(s.duration.Duration(), func() {
		// The event loop will execute onLocalTimeout for us.
		s.cancelCtx()
		s.eventLoop.AddEvent(TimeoutEvent{s.currentView}) // 这个timeout只在start的时候执行一次？
	})

	go func() {
		<-ctx.Done()   // 需要等待ctx.Done()信号
		s.timer.Stop() // 这里调用stop可以取消调用AddEvent(TimeoutEvent{})
	}()

	// start the initial proposal
	// Leader首先提出genesisblock
	if s.currentView == 1 && s.leaderRotation.GetLeader(s.currentView) == s.opts.ID() {
		s.consensus.Propose(s.SyncInfo())
	}
}

// HighQC returns the highest known QC.
func (s *Synchronizer) HighQC() hotstuff.QuorumCert {
	return s.highQC
}

// LeafBlock returns the current leaf block.
func (s *Synchronizer) LeafBlock() *hotstuff.Block {
	return s.leafBlock
}

// View returns the current view.
// 返回当前的view到底是怎么同步的？
func (s *Synchronizer) View() hotstuff.View {
	return s.currentView
}

// ViewContext returns a context that is cancelled at the end of the view.
// ViewContext返回当前view的上下文，在每个view的最后被cancelled
func (s *Synchronizer) ViewContext() context.Context {
	return s.viewCtx
}

// SyncInfo returns the highest known QC or TC.
func (s *Synchronizer) SyncInfo() hotstuff.SyncInfo {
	return hotstuff.NewSyncInfo().WithQC(s.highQC).WithTC(s.highTC)
}

// OnLocalTimeout is called when a local timeout happens.
func (s *Synchronizer) OnLocalTimeout() {
	// Reset the timer and ctx here so that we can get a new timeout in the same view.
	// I think this is necessary to ensure that we can keep sending the same timeout message
	// until we get a timeout certificate.
	// 重置timer和ctx，这样可以在同一个view中获取一个新的timeout；
	// 要持续广播相同的timeout消息？直到获得超时证书？

	// TODO: figure out the best way to handle this context and timeout.
	// 如果view的上下文报错不是nil（取消、超时），则要重新按duration创建上下文
	if s.viewCtx.Err() != nil {
		s.newCtx(s.duration.Duration())
	}
	// 本地超时的时候，先重置timer
	s.timer.Reset(s.duration.Duration())

	// 判断节点在当前view中否重复出现timeout：如果出现，则继续广播TimeoutMsg
	//（1）s.lastTimeout不是nil说明在一个view中已经执行过timeout
	//（2）如果s.lastTimeout的view和节点当前的view相等
	if s.lastTimeout != nil && s.lastTimeout.View == s.currentView {
		s.configuration.Timeout(*s.lastTimeout)
		return
	}

	// 下面的代码对节点在一个view中第一次出现timeout的情况做处理
	s.duration.ViewTimeout() // increase the duration of the next view
	view := s.currentView
	s.logger.Debugf("OnLocalTimeout: %v", view)

	// 对view进行签名
	sig, err := s.crypto.Sign(view.ToBytes())
	if err != nil {
		s.logger.Warnf("Failed to sign view: %v", err)
		return
	}
	timeoutMsg := hotstuff.TimeoutMsg{
		ID:            s.opts.ID(),
		View:          view,
		SyncInfo:      s.SyncInfo(),
		ViewSignature: sig,
	}

	if s.opts.ShouldUseAggQC() {
		// generate a second signature that will become part of the aggregateQC
		sig, err := s.crypto.Sign(timeoutMsg.ToBytes())
		if err != nil {
			s.logger.Warnf("Failed to sign timeout message: %v", err)
			return
		}
		timeoutMsg.MsgSignature = sig
	}
	// 将当前构建的timeoutMsg赋值给s.lastTimeout
	s.lastTimeout = &timeoutMsg
	// stop voting for current view
	// 在节点本地停止vote操作？如果已经vote了怎么办？
	// 将currentView赋值给lastVote，使得节点不会再执行投票
	s.consensus.StopVoting(s.currentView)

	// 广播timeoutMsg消息
	s.configuration.Timeout(timeoutMsg)
	s.logger.Infof("Synchronizer: OnLocalTimeout() -> OnRemoteTimeout()")
	s.OnRemoteTimeout(timeoutMsg)
}

// OnRemoteTimeout handles an incoming timeout from a remote replica.
// 收到TimeoutMsg处理事件，只有Leader会处理？还是所有Replica都会处理？
func (s *Synchronizer) OnRemoteTimeout(timeout hotstuff.TimeoutMsg) {
	s.logger.Infof("Synchronizer: OnRemoteTimeout() -> run AdvanceView")
	// 在方法最后清空小于currentViwe的timeout消息
	defer func() {
		// cleanup old timeouts
		for view := range s.timeouts {
			if view < s.currentView {
				delete(s.timeouts, view)
			}
		}
	}()

	verifier := s.crypto
	if !verifier.Verify(timeout.ViewSignature, timeout.View.ToBytes()) {
		return
	}
	s.logger.Debug("OnRemoteTimeout: ", timeout)

	// 本地按timeout的同步消息推进view
	s.AdvanceView(timeout.SyncInfo)

	// 在s.timeouts中记录timeout消息
	timeouts, ok := s.timeouts[timeout.View]
	if !ok {
		timeouts = make(map[hotstuff.ID]hotstuff.TimeoutMsg)
		s.timeouts[timeout.View] = timeouts
	}

	if _, ok := timeouts[timeout.ID]; !ok {
		timeouts[timeout.ID] = timeout
	}
	// 如果节点收到的timeout消息不超过quorum，不会构建timeoutQC
	if len(timeouts) < s.configuration.QuorumSize() {
		return
	}

	// TODO: should probably change CreateTimeoutCert and maybe also CreateQuorumCert
	// to use maps instead of slices
	timeoutList := make([]hotstuff.TimeoutMsg, 0, len(timeouts))
	for _, t := range timeouts {
		timeoutList = append(timeoutList, t)
	}

	tc, err := s.crypto.CreateTimeoutCert(timeout.View, timeoutList)
	if err != nil {
		s.logger.Debugf("Failed to create timeout certificate: %v", err)
		return
	}

	si := s.SyncInfo().WithTC(tc)

	if s.opts.ShouldUseAggQC() {
		aggQC, err := s.crypto.CreateAggregateQC(s.currentView, timeoutList)
		if err != nil {
			s.logger.Debugf("Failed to create aggregateQC: %v", err)
		} else {
			si = si.WithAggQC(aggQC)
		}
	}

	delete(s.timeouts, timeout.View)
	// 为什么这里在构建了timeoutQC之后要再发一次推进view？
	s.AdvanceView(si)
}

// Leader处理hotstuff.NewViewMsg消息
// OnNewView handles an incoming consensus.NewViewMsg
func (s *Synchronizer) OnNewView(newView hotstuff.NewViewMsg) {
	s.AdvanceView(newView.SyncInfo)
}

// AdvanceView attempts to advance to the next view using the given QC.
// qc must be either a regular quorum certificate, or a timeout certificate.
// 这里传入的是newView的SyncInfo
func (s *Synchronizer) AdvanceView(syncInfo hotstuff.SyncInfo) {
	v := hotstuff.View(0)
	timeout := false

	// check for a TC (timeout cert)
	// 超时签名是干嘛用的？
	if tc, ok := syncInfo.TC(); ok {
		if !s.crypto.VerifyTimeoutCert(tc) {
			s.logger.Info("Timeout Certificate could not be verified!")
			return
		}
		s.updateHighTC(tc)
		v = tc.View()
		timeout = true
	}

	var (
		haveQC bool
		qc     hotstuff.QuorumCert
		aggQC  hotstuff.AggregateQC
	)

	// check for an AggQC or QC
	if aggQC, haveQC = syncInfo.AggQC(); haveQC && s.opts.ShouldUseAggQC() {
		highQC, ok := s.crypto.VerifyAggregateQC(aggQC)
		if !ok {
			s.logger.Info("Aggregated Quorum Certificate could not be verified")
			return
		}
		if aggQC.View() >= v {
			v = aggQC.View()
			timeout = true
		}
		// ensure that the true highQC is the one stored in the syncInfo
		syncInfo = syncInfo.WithQC(highQC)
		qc = highQC
		// s.logger.Infof("checkQC: view=%d", s.currentView) // default run不走这一步
	} else if qc, haveQC = syncInfo.QC(); haveQC { // 验证同步数据里的qc，
		if !s.crypto.VerifyQuorumCert(qc) {
			s.logger.Info("Quorum Certificate could not be verified!")
			return
		}
	}

	// replica发rpc给leader的时候会带着QC
	if haveQC {
		s.updateHighQC(qc)
		// if there is both a TC and a QC, we use the QC if its view is greater or equal to the TC.
		if qc.View() >= v {
			v = qc.View()
			timeout = false
		}
	}

	// s.logger.Infof("Call advanceView(), ID: %d, v: %d, currentView: %d", s.opts.ID(), v, s.currentView)

	// 对timeout事件，
	// 如果最高QC.view小于当前view，则不会考虑推进；如果QC.view追赶上了当前view，则一定要推进
	if v < s.currentView {
		return
	}

	s.timer.Stop()
	// 如果没有触发timeout，则可以计算view duration，从viewDuration.startTime开始算起
	if !timeout {
		s.duration.ViewSucceeded()
	}

	s.currentView = v + 1 // 这里的currentView改变后，其他模块引用的synchronizer模块的view是否会更新？（可以更新）
	s.lastTimeout = nil
	s.duration.ViewStarted()

	duration := s.duration.Duration()
	// cancel the old view context and set up the next one
	s.newCtx(duration)
	s.timer.Reset(duration)

	s.logger.Debugf("advanced to view %d", s.currentView)
	// ViewChangeEvent的处理在./metrics/timeouts.go，处理只做了numViews++
	s.eventLoop.AddEvent(ViewChangeEvent{View: s.currentView, Timeout: timeout})

	// RapidFair: baseline 使用参数控制在order fairness模式下调用collect
	// 先collect，之后再propose
	leader := s.leaderRotation.GetLeader(s.currentView)
	if leader == s.opts.ID() { // 如果当前节点是leader
		s.logger.Infof("leader advanced to view %d", s.currentView)
		if s.opts.UseFairOrder() {
			s.logger.Debugf("[FairOrder] AdvanceView(): View in synchronizer: %d", s.currentView)
			// leader广播ReadyCollect通知replicas可以发送txSeq
			s.col.ReadyCollect(syncInfo)
		} else {
			s.consensus.Propose(syncInfo) // 推进view的时候leader节点就直接调用propose方法了
		}
	} else if replica, ok := s.configuration.Replica(leader); ok {
		s.logger.Debugf("Replica advanced to view %d", s.currentView)
		replica.NewView(syncInfo) // 这里调用了./backend/config.go中的NewView
	}
	// fmt.Println("New Advanced View duration:", duration)
}

// updateHighQC attempts to update the highQC, but does not verify the qc first.
// This method is meant to be used instead of the exported UpdateHighQC internally
// in this package when the qc has already been verified.
func (s *Synchronizer) updateHighQC(qc hotstuff.QuorumCert) {
	newBlock, ok := s.blockChain.Get(qc.BlockHash())
	if !ok {
		s.logger.Info("updateHighQC: Could not find block referenced by new QC!")
		return
	}

	oldBlock, ok := s.blockChain.Get(s.highQC.BlockHash())
	if !ok {
		s.logger.Panic("Block from the old highQC missing from chain")
	}

	if newBlock.View() > oldBlock.View() {
		s.highQC = qc
		s.leafBlock = newBlock
		s.logger.Debug("HighQC updated")
	}
}

// updateHighTC attempts to update the highTC, but does not verify the tc first.
func (s *Synchronizer) updateHighTC(tc hotstuff.TimeoutCert) {
	if tc.View() > s.highTC.View() {
		s.highTC = tc
		s.logger.Debug("HighTC updated")
	}
}

// 以duration为时间间隔创建当前view的上下文，同时设置超时时间
func (s *Synchronizer) newCtx(duration time.Duration) {
	s.cancelCtx()
	s.viewCtx, s.cancelCtx = context.WithTimeout(context.Background(), duration)
}

var _ modules.Synchronizer = (*Synchronizer)(nil)

// ViewChangeEvent is sent on the eventloop whenever a view change occurs.
type ViewChangeEvent struct {
	View    hotstuff.View
	Timeout bool
}

// TimeoutEvent is sent on the eventloop when a local timeout occurs.
type TimeoutEvent struct {
	View hotstuff.View
}
