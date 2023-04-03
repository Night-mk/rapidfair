package optimisticfairorder

import (
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
	eventLoop        *eventloop.EventLoop     // 处理事件
	acceptor         modules.Acceptor
	commandQueue     modules.CommandQueue
	opts             *modules.Options
	configuration    modules.Configuration
	col              modules.Collector
	crypto           modules.Crypto

	lastVote hotstuff.View

	// mut   sync.Mutex
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
		&of.eventLoop,
		&of.acceptor,
		&of.commandQueue,
		&of.opts,
		&of.configuration,
		&of.col,
		&of.crypto,
	)

	// 注册消息处理
	// 处理CollectMsg，注册事件处理函数OnMultiCollect
	of.eventLoop.RegisterHandler(hotstuff.MultiCollectMsg{}, func(event any) { of.OnMutiCollect(event.(hotstuff.MultiCollectMsg)) })

	// 处理PreNotifyMsg，注册事件处理函数OnPreNotify
	of.eventLoop.RegisterHandler(hotstuff.PreNotifyMsg{}, func(event any) { of.OnPreNotify(event.(hotstuff.PreNotifyMsg)) })
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
		// if qcFragment, ok := of.fragmentChain.Get(qc.BlockHash()); ok {
		if fragment, ok := of.fragmentChain.LocalGetAtHeight(qc.View() + 1); ok {
			// 在cmdcache中提交virView的fragment并准备好构建下一个txbatch
			// of.acceptor.Proposed(qcFragment.OrderedTx())
			of.acceptor.ProposedFragment(fragment.OrderedTx())
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
		of.OnMutiCollect(mcmsg)
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
	// of.logger.Infof("[OnMutiCollect]")

	// 1. 对比msg中的virView和本地同步器中的fragment的virView
	txSeq := mc.MultiCollect
	msgVirView := txSeq.VirView()
	// highQcView应该要维护TxSeqFragment的view来更新！！！
	// 但是此时virView的关系是：highQcView = curTFSVirView - 1 = msgVirView - 2
	highQcView := of.fairSynchronizer.LeafTSF().VirView()

	// 消息中的view如果比最新的上一个节点的view更低，则不接受该消息
	if msgVirView <= highQcView+1 {
		return
	}
	// 如果msgVirView比本地的最高highQcView+1还高，应该使用msgVirView作为新的virView
	newVirView := msgVirView
	// of.logger.Infof("[OnMultiCollect]: After Get Replica ID on replicaID: %d, virView: %d", of.opts.ID(), msgVirView)
	// 2. 缓存其他节点广播的交易序列到本地缓存txLists: map<VirView,TXList=<ID,Command>>
	var votes []hotstuff.PartialCert
	cert := txSeq.PartialCert()
	if msgVirView == newVirView {
		// 验证cert的正确性
		if !of.crypto.VerifyPartialCertTSF(cert) {
			of.logger.Info("[OnMutiCollect]: TSFVote could not be verified!")
			return
		}
		// 缓存replica发送的交易序列
		if len(of.txLists[newVirView]) == 0 {
			of.txLists[newVirView] = make(hotstuff.TXList)
		}
		of.txLists[newVirView][mc.ID] = txSeq.TxSeq()
		// 同时也要缓存TxSeqFragment的PC
		votes = of.tsfVote[cert.BlockHash()]
		votes = append(votes, cert)
		of.tsfVote[cert.BlockHash()] = votes
	} else {
		return
	}

	// 3. 对于不同节点接收到collectMsg后的处理
	// 计算virView=v+1的leader节点的ID
	leaderID := of.leaderRotation.GetLeader(newVirView)
	if leaderID == of.opts.ID() {
		// 3.1 Leader的处理 -> Fair Ordering
		// Leader接收到(n-f)个交易序列后就可以开始PreNotify+公平排序
		if len(of.txLists[newVirView]) != of.configuration.QuorumSizeFair() {
			return
		}
		of.logger.Infof("[OnMutiCollect]: Leader process, new virtual view: %d", newVirView)
		// 3.2 计算TxSeqFragment(v-1)的QC，方法返回后删除本地缓存的PC
		// 根据PC.hash从tsfChain获取TxSeqFragment
		tsfPrev, ok := of.tsfChain.Get(cert.BlockHash())
		if !ok {
			of.logger.Debugf("Could not find TxSeqFragment for vote: %.8s.", cert.BlockHash())
			return
		}
		// 利用(n-f)个PC计算TxSeqFragment(v-1)的QC
		qc, err := of.crypto.CreateQuorumCertTSF(tsfPrev, votes)
		if err != nil {
			of.logger.Info("OnVote: could not create QC for TxSeqFragment: ", err)
			return
		}
		delete(of.tsfVote, cert.BlockHash())
		// 使用QC构建vsync
		vsyncNew := hotstuff.NewSyncInfo().WithQC(qc)
		// 3.3 Leader使用FairSynchronizer推进virtual view
		of.eventLoop.AddEvent(hotstuff.NewVirViewMsg{
			ID:    of.opts.ID(),
			VSync: vsyncNew,
		})
		// of.fairSynchronizer.AdvanceVirView(vsyncNew)

	}
	// Replica的处理（不操作，等待PreNotify?）
	// 直接进入到Pre-Verify阶段

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
	of.logger.Infof("[PreNotify]: tsf hash: %.8s", tsfCur.Hash())
	// 2. 存储TxSeqFragment(v)到
	of.tsfChain.Store(tsfCur)
	// 3. 广播PreNotify消息
	of.configuration.PreNotify(pn)
	// 4. Leader本地调用OnPreNotify
	of.OnPreNotify(pn)
}

// 处理接收的PreNotifyMsg（类似OnPropose）
func (of *optFairOrder) OnPreNotify(pnm hotstuff.PreNotifyMsg) {
	of.logger.Debugf("[OnPreNotify]")
	// of.logger.Infof("[OnPreNotify]")
	// 1. 先判断PreNotifyMsg的正确性
	tsf := pnm.TxSeqFragment
	// 1.1 验证QC
	if !of.crypto.VerifyQuorumCertTSF(tsf.QuorumCert()) {
		of.logger.Info("[OnPreNotify]: Invalid QC")
		return
	}
	// 1.2 验证leader是否正确
	if pnm.ID != of.leaderRotation.GetLeader(tsf.VirView()) {
		of.logger.Infof("[OnPreNotify]: Invalid virtual leader")
		return
	}
	// 一般情况下再这里应该要提交本virView的Fragment(v)，但是我们的协议想并行处理公平排序的计算，所以replica在这里先不提交，等到replica本地计算完公平排序，在MultiCollect中再提交
	// 1.3 只验证QC.TxSeqFragment是否能获取
	if _, ok := of.tsfChain.Get(tsf.QuorumCert().BlockHash()); !ok {
		of.logger.Infof("[OnPreNotify]: Failed to fetch QC.TxSeqFragment")
	}
	// 1.3.1 能获取到QC时，直接存储PreNotifyMsg发送的tsf到本地
	of.tsfChain.Store(tsf)

	// 1.4 判断发送来的tsf是否已经提交过
	if tsf.VirView() <= of.lastVote {
		of.logger.Infof("[OnPreNotify]: TSF view too old, TSF view: %d, current view: %d, lastVote view: %d", tsf.VirView(), of.fairSynchronizer.VirView(), of.lastVote)
		return
	}

	// 2. replicas计算对tsf的部分签名pc(表示同意tsf)
	pc, err := of.crypto.CreatePartialCertTSF(tsf)
	if err != nil {
		of.logger.Error("OnPropose: failed to sign TxSeqFragment: ", err)
		return
	}
	// 更新lastVote
	of.lastVote = tsf.VirView()

	// 计算tsf的当前的leader
	// curLeaderID := of.leaderRotation.GetLeader(tsf.VirView())
	// if curLeaderID != of.opts.ID() {}

	// 3. 当前VirView()的replica（包括leader）根据tsf指定的节点的交易序列TxSeqHash，在本地计算公平排序
	// 本地计算公平排序
	odc := of.col.FairOrder(tsf.TxSeqHash())
	// 构建fragment
	fragment := hotstuff.NewFragment(tsf.VirView(), tsf.VirLeader(), odc, tsf.TxSeqHash(), hotstuff.Command(""), make(hotstuff.TXList))
	// 将fragment存储到fragmentChain
	of.fragmentChain.Store(fragment)
	// 将fragment增加到cmdCache的缓存中
	of.commandQueue.AddFragment(fragment)

	// 4. 如果没有推进view，则在defer方法中最后推进节点的view（只有current replica会推进）
	didAdvanceView := false
	vsync := hotstuff.NewSyncInfo().WithQC(tsf.QuorumCert())
	// defer func() {
	// 在执行multiCollect之前就推进virView
	if !didAdvanceView {
		// of.logger.Infof("[OnPreNotify]: Replica call advanceView() actively, lastvote view: %d", of.lastVote)
		of.fairSynchronizer.AdvanceVirView(vsync)
	}
	// }()

	// 5. 清空本virView接收的交易序列（清理缓存）
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
