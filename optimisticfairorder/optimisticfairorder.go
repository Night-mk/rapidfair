package optimisticfairorder

import (
	"sync"
	"time"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
)

// 实现解耦的乐观公平排序的主体流程
// optimistic fair order engine

// 功能梳理
/*
	1. 控制multiCollect
		1.1 从cmdcache获取交易序列local-order
		1.2 广播local-order给所有replica
		1.3 收到local-order后，在OnMultiCollect中处理消息
	2. 控制
*/

type optFairOrder struct {
	fragmentChain    modules.FragmentChain // 维护fragment
	tsfChain         modules.TSFChain
	leaderRotation   modules.LeaderRotation // 计算virView对应的交易virLeader
	logger           logging.Logger
	fairSynchronizer modules.FairSynchronizer // 利用synchronizer控制推进virView
	// eventLoop        *eventloop.EventLoop     // 处理事件
	eventLoopFair *eventloop.EventLoopFair // 处理事件
	acceptor      modules.Acceptor
	commandQueue  modules.CommandQueue
	opts          *modules.Options
	configuration modules.Configuration
	col           modules.Collector
	crypto        modules.Crypto

	lastVote hotstuff.View

	mut   sync.Mutex
	fExec *hotstuff.Fragment // 记录已经完成构建的最新fragment

	txLists map[hotstuff.View]hotstuff.TXList
	tsfVote map[hotstuff.Hash][]hotstuff.PartialCert // 缓存TxSeqFragment的部分签名PartialCert
}

func New() modules.OptimisticFairOrder {
	return &optFairOrder{
		lastVote: 0,
		fExec:    hotstuff.GetGenesisFragment(),
		txLists:  make(map[hotstuff.View]hotstuff.TXList),
		tsfVote:  make(map[hotstuff.Hash][]hotstuff.PartialCert),
	}
}

// init modules
func (of *optFairOrder) InitModule(mods *modules.Core) {
	mods.Get(
		&of.fragmentChain,
		&of.tsfChain,
		&of.leaderRotation,
		&of.logger,
		&of.fairSynchronizer,
		&of.eventLoopFair,
		&of.acceptor,
		&of.commandQueue,
		&of.opts,
		&of.configuration,
		&of.col,
		&of.crypto,
	)

	// 注册消息处理
	// 处理CollectMsg，注册事件处理函数OnMultiCollect
	of.eventLoopFair.RegisterHandler(hotstuff.MultiCollectMsg{}, func(event any) { of.OnMutiCollect(event.(hotstuff.MultiCollectMsg)) })

	// 处理PreNotifyMsg，注册事件处理函数OnPreNotify
	of.eventLoopFair.RegisterHandler(hotstuff.PreNotifyMsg{}, func(event any) { of.OnPreNotify(event.(hotstuff.PreNotifyMsg)) })
}

// 控制multiCollect，广播local-order的交易序列给所有replica
// vsync表示virtual view的同步消息
// 其中vsync = {qc={signature=QuorumCert, view=virView, hash=fragmentHash}}
// 这里的QuorumCert是对TxSeqFragment(v)的阈值签名
func (of *optFairOrder) MultiCollect(vsync hotstuff.SyncInfo, pc hotstuff.PartialCert) {
	// of.logger.Info("[MultiCollect]")
	// 1. 使用OnPreNotify传输来的vsync，来更新cmdcache中的当前选择的交易序列
	// 获取最新的QC中的virView
	v := hotstuff.View(0)
	var (
		haveQC bool
		qc     hotstuff.QuorumCert
	)
	if qc, haveQC = vsync.QC(); haveQC {
		// 调用MultiCollect时，节点同步器的virView已经更新了，正常情况是QC.VirView=vsync.VirView-1，所以如果收到的virView<vsync.VirView-1, 则直接返回
		if qc.View() < of.fairSynchronizer.VirView()-1 {
			return
		}
		// 1.1 验证QC
		if !of.crypto.VerifyQuorumCertTSF(qc) {
			of.logger.Info("[MultiCollect]: Quorum Certificate could not be verified!")
			return
		}
		// 1.2 在cmdcache中确定提交最新QC的virView中的fragment
		// 通过TxSeqFragment的QC.View()=v获取对应View的Fragment(v)，Fragment此时还没构建QC
		// 开始执行collect阶段的节点一定能从本地获取qc.virView+1的fragment，因为这些节点已经计算完成了公平排序
		// if qcFragment, ok := of.fragmentChain.Get(qc.BlockHash()); ok {
		if fragment, ok := of.fragmentChain.LocalGetAtHeight(qc.View() + 1); ok {
			// 在cmdcache中提交virView的fragment并准备好构建下一个txbatch
			of.acceptor.ProposedFragment(fragment.VirView(), fragment.OrderedTx())
		} else {
			of.logger.Infof("Could not find fragment for virView=QC.VirView+1: %d", qc.View()+1)
		}
	}
	// 使用vsync的最高QC，或者最高TC的virView来更新v
	if haveQC {
		if qc.View() >= v {
			v = qc.View()
		}
	}

	// 2. 从cmdcache中获取txBatch
	cmd, ok := of.commandQueue.GetTxBatch(of.fairSynchronizer.VirViewContext())
	if !ok {
		of.logger.Debug("[MultiCollect] Collect: No command")
		return
	}
	// 3. 构建MultiCollectMsg消息，并广播（vote给下一个virLeader）
	newVirView := v + 2 // 此时replica的同步器的virtual view已经了更新，所以新的virtual view应该是QC+2
	// of.logger.Infof("[MultiCollect]: new virtual view: %d", newVirView)
	mcmsg := hotstuff.MultiCollectMsg{
		ID: of.opts.ID(),
		MultiCollect: hotstuff.NewMultiCollect(
			newVirView,
			cmd, // virView = v+1时用于排序的交易序列txList
			pc,  // 直接使用OnPreNotify传入的TxSeqFragment(v)的PC
		),
	}
	leaderID := of.leaderRotation.GetLeader(newVirView)
	if of.opts.ID() == leaderID {
		// leader本地触发OnMutiCollect事件
		of.eventLoopFair.AddEvent(mcmsg)
		// of.OnMutiCollect(mcmsg)
	} else {
		// 所有replica都发送MultiCollectMsg给leader
		replica, ok := of.configuration.Replica(leaderID)
		if !ok {
			of.logger.Infof("[MultiCollect]: Replica with ID %d was not found!", leaderID)
			return
		}
		replica.MultiCollect(mcmsg)
	}

}

// 处理接收的MultiCollectMsg（类似OnVote）
// 此时QC(v-2), curVirView(v-1), newVirView(v)
func (of *optFairOrder) OnMutiCollect(mc hotstuff.MultiCollectMsg) {
	// of.logger.Infof("[OnMutiCollect] Start, receive from %d", mc.ID)

	// 1. 对比msg中的virView和本地同步器中的fragment的virView
	txSeq := mc.MultiCollect
	msgVirView := txSeq.VirView()
	// highQcView应该要维护TxSeqFragment的view来更新！！！
	// 但是此时virView的关系是：highQcView = curTSFVirView - 1 = msgVirView - 2
	highQcView := of.fairSynchronizer.LeafTSF().VirView()

	// 消息中的view如果比最新的上一个节点的view更低，则不接受该消息
	if msgVirView <= highQcView+1 {
		return
	}
	// 如果msgVirView比本地的最高highQcView+1还高，应该使用msgVirView作为新的virView
	newVirView := msgVirView

	quit := make(chan int)
	// Leader并行执行接收到MultiCollect之后的处理工作
	go of.handleCollect(newVirView, mc, quit)
	// of.handleCollect(newVirView, cert, mc)

	// 阻塞主程序等待协程返回
	for {
		<-quit
		close(quit)
		break
	}
	// of.logger.Infof("[OnMutiCollect] Async END, receive from: %d, len of.txLists: %d", mc.ID, len(of.txLists[newVirView]))
}

// 使用goroutine需要阻塞主线程
func (of *optFairOrder) handleCollect(newVirView hotstuff.View, mc hotstuff.MultiCollectMsg, quit chan int) {
	of.mut.Lock()
	defer of.mut.Unlock()

	var votes []hotstuff.PartialCert
	// var curTxLists hotstuff.TXList
	cert := mc.MultiCollect.PartialCert()

	// 缓存replica发送的交易序列
	if len(of.txLists[newVirView]) == 0 {
		of.txLists[newVirView] = make(hotstuff.TXList)
	}
	needAdd := false
	// 先判断是否需要继续收集其他节点发送的数据，之后再验证PC
	if len(of.txLists[newVirView]) < of.configuration.QuorumSizeFair() {
		// 验证PC的正确性 （耗时比较长，batchsize=100, 时间约为0.2~1ms±）
		if !of.crypto.VerifyPartialCertTSF(cert) {
			of.logger.Infof("[OnMutiCollect]: TSFVote pc could not be verified! for pc virView: %d", newVirView-1)
			return
		}
		needAdd = true
		// 缓存其他节点广播的交易序列到本地缓存txLists: map<VirView,TXList=<ID,hash(Command)>>
		of.txLists[newVirView][mc.ID] = mc.MultiCollect.TxSeq()
		// 同时也要缓存TxSeqFragment的PC
		votes = of.tsfVote[cert.BlockHash()]
		votes = append(votes, cert)
		of.tsfVote[cert.BlockHash()] = votes
	}

	// of.logger.Infof("[OnMutiCollect]: receive from %d, new virtual view: %d, len txLists[newView]: %d, len votes:%d", mc.ID, newVirView, len(of.txLists[newVirView]), len(of.tsfVote[cert.BlockHash()]))

	// Leader接收到(n-f)个交易序列后就可以开始PreNotify+公平排序
	if len(of.txLists[newVirView]) < of.configuration.QuorumSizeFair() {
		quit <- 1
		return
	}
	if !needAdd {
		// of.logger.Infof("[OnMutiCollect]: return virtual view: %d\n", newVirView)
		quit <- 1
		return
	}

	// 3.2 计算TxSeqFragment(v-1)的QC，方法返回后删除本地缓存的PC
	// 根据PC.hash从tsfChain获取TxSeqFragment
	tsfPrev, ok := of.tsfChain.Get(cert.BlockHash())
	if !ok {
		of.logger.Infof("Could not find TxSeqFragment for vote: %.8s.", cert.BlockHash())
		return
	}
	// 利用(n-f)个PC计算TxSeqFragment(v-1)的QC（createQC的时间很短，1-10μm）
	qc, err := of.crypto.CreateQuorumCertTSF(tsfPrev, votes)
	if err != nil {
		of.logger.Info("OnVote: could not create QC for TxSeqFragment: ", err)
		return
	}

	delete(of.tsfVote, cert.BlockHash())
	// 使用QC构建vsync
	vsyncNew := hotstuff.NewSyncInfo().WithQC(qc)
	// 3.3 Leader使用FairSynchronizer推进virtual view
	of.eventLoopFair.AddEvent(hotstuff.NewVirViewMsg{
		ID:    of.opts.ID(),
		VSync: vsyncNew,
	})
	// of.logger.Infof("[OnMutiCollect]: Leader process, new virtual view: %d", newVirView)
	if newVirView%20 == 0 {
		of.logger.Infof("[OnMutiCollect]: Leader process, new virtual view: %d", newVirView)
	}
	quit <- 1
}

// Leader：广播PreNotify消息给replicas（类似Propose）
// 调用时leader所处的virView = QC(v-1).VirView + 1 = v
func (of *optFairOrder) PreNotify(vsync hotstuff.SyncInfo) {
	// 获取vsync的QC，此时leader已经完成了QC计算，并推进了virView
	// of.logger.Infof("[PreNotify]")

	qc, ok := vsync.QC()
	if ok {
		// 确定上一个TxSeqFragment(v-1)是否已经保存
		if _, ok := of.tsfChain.Get(qc.BlockHash()); !ok {
			of.logger.Errorf("Could not find TxSeqFragment for QC: %s", qc)
			of.logger.Infof("Could not find TxSeqFragment for QC: %s", qc)
		}
	}

	// Leader使用本地同步器的VirView()作为最新消息的VirView
	newVirView := of.fairSynchronizer.VirView()
	// 广播PreNotify消息给replicas
	// 1. 构建PreNotify消息
	// 1.1 针对virView=1的Fragment，直接采用空的txList写入fragment
	var curTxLists hotstuff.TXList
	if newVirView == 1 {
		curTxLists = make(hotstuff.TXList)
	} else {
		curTxLists = of.txLists[newVirView]
	}
	// 1.2 使用QC.hash作为parent，使用leader接收的交易序列of.txLists[newVirView]，构建新的TxSeqFragment(v)
	// 但是在Genesis时，这里本地的交易序列是null，不能用null来公平排序呀？
	tsfCur := hotstuff.NewTxSeqFragment(
		qc.BlockHash(), qc, newVirView, of.opts.ID(), curTxLists,
	)
	pn := hotstuff.PreNotifyMsg{
		ID:            of.opts.ID(),
		TxSeqFragment: tsfCur,
	}
	// of.logger.Infof("[PreNotify]: tsf hash: %.8s", tsfCur.Hash())

	// 2. 存储TxSeqFragment(v)到tsfChain
	// of.tsfChain.Store(tsfCur)
	// 3. 广播PreNotify消息
	go of.configuration.PreNotify(pn)
	// 4. Leader本地调用OnPreNotify
	of.OnPreNotify(pn)
}

// 处理接收的PreNotifyMsg（类似OnPropose）
func (of *optFairOrder) OnPreNotify(pnm hotstuff.PreNotifyMsg) {
	of.logger.Debugf("[OnPreNotify]")
	// of.logger.Infof("[OnPreNotify]")
	// 1. 先判断PreNotifyMsg的正确性
	tsf := pnm.TxSeqFragment
	// 1.1 验证QC（batchsize=100, 时间约为0.1~4ms±）
	if !of.crypto.VerifyQuorumCertTSF(tsf.QuorumCert()) {
		of.logger.Info("[OnPreNotify]: Invalid QC")
		return
	}

	// 1.2 验证leader是否正确
	if pnm.ID != of.leaderRotation.GetLeader(tsf.VirView()) {
		of.logger.Infof("[OnPreNotify]: Invalid virtual leader")
		return
	}
	// 1.3 判断发送来的tsf是否已经提交过
	if tsf.VirView() <= of.lastVote {
		of.logger.Infof("[OnPreNotify]: TSF view too old, TSF view: %d, current view: %d, lastVote view: %d", tsf.VirView(), of.fairSynchronizer.VirView(), of.lastVote)
		return
	}
	// 一般情况下再这里应该要提交本轮virView的Fragment(v)，但是我们的协议想并行处理公平排序的计算，所以replica在这里先不提交，等到replica本地计算完公平排序，在MultiCollect中再提交
	// 1.4 验证QC.TxSeqFragment是否能获取
	if tsf, ok := of.tsfChain.Get(tsf.QuorumCert().BlockHash()); ok {
		// 如果能在节点本地获取构建了QC的TxSeqFragment
		qcVirView := tsf.VirView()
		if qcVirView > 0 { // genesis的fragment已经在初始化的时候写入了
			// 之后才能再将tsf对应的fragment增加到节点的cmdCache的缓存中
			// eventloop保证一个节点一定会先执行完上一个OnPreNotify将fragment存入fragmentChain，再执行下一个OnPreNotify
			if fragment, ok := of.fragmentChain.LocalGetAtHeight(qcVirView); ok {
				// of.logger.Infof("[OnPreNotify]: Can Add fragment on virView: %d", qcVirView)
				of.commandQueue.AddFragment(fragment)
			} else {
				of.logger.Infof("[OnPreNotify]: error Failed to get QC.Fragment")
			}
		}
	} else {
		of.logger.Infof("[OnPreNotify]: error Failed to get QC.TxSeqFragment")
		return
	}
	// 1.5 能获取到QC时，直接存储PreNotifyMsg发送的tsf到tsfChain
	of.tsfChain.Store(tsf)

	// 2. replicas计算对tsf的部分签名pc(表示同意tsf)（batchsize=100, 时间约为0.1~3ms±）
	pc, err := of.crypto.CreatePartialCertTSF(tsf)

	if err != nil {
		// of.logger.Error("[OnPreNotify]: failed to sign TxSeqFragment: ", err)
		of.logger.Info("[OnPreNotify]: failed to sign TxSeqFragment: ", err)
		return
	}
	// 更新lastVote
	of.lastVote = tsf.VirView()

	// 3. 当前VirView()的replica（包括leader）根据tsf指定的节点的交易序列TxSeqHash，在本地计算公平排序
	// 本地计算公平排序
	start := time.Now() // 获取当前时间
	odc := of.col.FairOrder(tsf.TxSeqHash())
	// 构建fragment
	fragment := hotstuff.NewFragment(tsf.VirView(), tsf.VirLeader(), odc, tsf.TxSeqHash(), hotstuff.Command(""), make(hotstuff.TXList))
	// 将fragment存储到fragmentChain
	of.fragmentChain.Store(fragment)
	// 将fragment增加到cmdCache的缓存中
	// of.commandQueue.AddFragment(fragment)
	elapsed1 := time.Since(start)
	curLeaderID := of.leaderRotation.GetLeader(tsf.VirView())
	// of.logger.Info("Replica Fair Order + new,store fragment time:", elapsed1)
	if curLeaderID == of.opts.ID() {
		// of.logger.Info("Virtual Leader Fair Order + new,store fragment time:", elapsed1)
		if tsf.VirView()%20 == 0 {
			of.logger.Info("Virtual Leader Fair Order + new,store fragment time:", elapsed1)
		}
	}

	// 4. 如果没有推进view，则在defer方法中最后推进节点的view（只有current replica会推进）
	didAdvanceView := false
	vsync := hotstuff.NewSyncInfo().WithQC(tsf.QuorumCert())
	// 在执行multiCollect之前就推进virView
	if !didAdvanceView {
		// of.logger.Infof("[OnPreNotify]: Replica call advanceView() actively, lastvote view: %d", of.lastVote)
		of.fairSynchronizer.AdvanceVirView(vsync)
	}
	// 5. Leader清空本virView接收的交易序列（清理缓存）
	if len(of.txLists) > 0 {
		for k := range of.txLists {
			if k <= of.fairSynchronizer.LeafTSF().VirView() {
				delete(of.txLists, k)
			}
		}
	}
	// of.logger.Infof("[OnPreNotify]: After Delete cache on replicaID: %d", of.opts.ID())

	// 6.所有replica都执行MutiCollect来处理新一轮的Collect消息发送
	of.MultiCollect(vsync, pc)
}
