package consensus

import (
	"sync"
	"time"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/consensus/orderfairness"
	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/internal/proto/clientpb"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
	"google.golang.org/protobuf/proto"
)

// 考虑增加额外的interface，为扩展并行工作做准备
// type CollectRule interface{
// }

// init() 用于 程序运行前 的进行包初始化（自定义变量、实例化通信连接）工作，初始化顺序是按照解析的依赖关系的顺序执行，没有依赖的包最先初始化。
// 如果存在不同版本的collect，在这里注册serialCollection module
// 在module/modules.go 增加interface，初始化也返回interface
func init() {
	modules.RegisterModule("serialcollect", NewCollectMachine)
}

// CollectMachine收集交易序列
type CollectMachine struct {
	acceptor       modules.Acceptor
	blockChain     modules.BlockChain
	consensus      modules.Consensus
	crypto         modules.Crypto
	configuration  modules.Configuration
	commandQueue   modules.CommandQueue
	eventLoop      *eventloop.EventLoop
	leaderRotation modules.LeaderRotation
	logger         logging.Logger
	synchronizer   modules.Synchronizer
	opts           *modules.Options

	mut            sync.Mutex
	collectedTxSeq map[hotstuff.View]map[hotstuff.ID]hotstuff.Command // 按view存储收集的交易序列列表
	// orderedCommand hotstuff.Command                                   // 存储公平排序后的交易序列
	marshaler   proto.MarshalOptions   // 序列化（来自proto）
	unmarshaler proto.UnmarshalOptions // 反序列化

	// 计算Themis的整体和computation latency
	gStartTime        time.Time
	totalLatency      float64
	totalOrderLatency float64
}

// 创建一个新NewCollectMachine
// 不一定需要构建一个interface，也不一定要注册模块？说起来应该只有存在不同版本的相似算法时需要注册模块进行指定配置
// func NewCollectMachine() *CollectMachine {
func NewCollectMachine() modules.Collector {
	return &CollectMachine{
		collectedTxSeq: make(map[hotstuff.View]map[hotstuff.ID]hotstuff.Command),
		marshaler:      proto.MarshalOptions{Deterministic: true},
		unmarshaler:    proto.UnmarshalOptions{DiscardUnknown: true},
	}
}

// 初始化模块，注册处理函数
func (cm *CollectMachine) InitModule(mods *modules.Core) {
	mods.Get(
		&cm.acceptor,
		&cm.blockChain,
		&cm.consensus,
		&cm.crypto,
		&cm.configuration,
		&cm.commandQueue,
		&cm.eventLoop,
		&cm.leaderRotation,
		&cm.logger,
		&cm.synchronizer,
		&cm.opts,
	)
	// 注册事件处理函数OnCollect
	cm.eventLoop.RegisterHandler(hotstuff.CollectMsg{}, func(event any) { cm.OnCollect(event.(hotstuff.CollectMsg)) })

	// 注册事件处理函数OnReadyCollect
	cm.eventLoop.RegisterHandler(hotstuff.ReadyCollectMsg{}, func(event any) { cm.OnReadyCollect(event.(hotstuff.ReadyCollectMsg)) })
}

// collection流程
// 1. replica从本地交易列表中提取batch-size的交易序列txSeq
// 2. replica将txSeq包装成消息CollectTxSeq
// 3. replica节点将CollectTxSeq发送给当前View的leader节点
func (cm *CollectMachine) Collect(syncInfo hotstuff.SyncInfo) {
	// 从hotstuff.NewViewMsg的同步信息的QC中获取同步消息中最高的view
	v := hotstuff.View(0)
	var (
		haveQC bool
		qc     hotstuff.QuorumCert
	)
	if tc, ok := syncInfo.TC(); ok {
		v = tc.View()
	}
	if qc, haveQC = syncInfo.QC(); haveQC { // 验证同步数据里的qc，
		if !cm.crypto.VerifyQuorumCert(qc) {
			cm.logger.Info("Quorum Certificate could not be verified!")
			return
		}
		// tell the acceptor that the previous proposal succeeded.
		// 在这里直接确定上一个提交的最高区块的交易已经在本地cache中更新
		// Get在这里也是先从本地获取区块，如果本地不存在则会从远程获取对应的区块
		if qcBlock, ok := cm.blockChain.Get(qc.BlockHash()); ok {
			// replicas(包括leader)在这里更新cmdcache中存储的上一个block的交易缓存
			cm.acceptor.Proposed(qcBlock.Command())
		} else {
			cm.logger.Infof("Could not find block for QC: %s", qc)
		}
	}
	if haveQC {
		if qc.View() >= v {
			v = qc.View()
		}
	}
	// cm.logger.Infof("Collect view of voted block: %d", v)

	// Collect方法调用时要使用上一个区块view+1作为collect当前的view
	currentView := v + 1
	// 在cmdcache.go设计新的GetTxBatch()，使得在获取batchtx进行collect的时候不会从cache中删除，只有收到proposal并通过后才从cache里删除
	cmd, ok := cm.commandQueue.GetTxBatch(cm.synchronizer.ViewContext())
	if !ok {
		cm.logger.Debug("[CollectMachine-Collect] Collect: No command")
		return
	}
	// 将batchsize大小的txSeq包装成消息CollectTxSeq
	colmsg := hotstuff.CollectMsg{
		ID: cm.opts.ID(),
		CollectTxSeq: hotstuff.NewCollectTxSeq(
			currentView,
			cmd,
			syncInfo,
		),
	}
	// 远程调用leader的Collect方法，去Oncollect处理
	// 获取当前View的leader
	leaderID := cm.leaderRotation.GetLeader(currentView)
	if leaderID == cm.opts.ID() { // 如果当前节点是leader
		cm.logger.Debugf("[CollectMachine-Collect] Leader: node sync currentView: %d, Collect View: %d, Collec View LeaderID: %d", cm.synchronizer.View(), currentView, leaderID)
		// 直接增加事件，交给oncollect()处理
		cm.eventLoop.AddEvent(colmsg)
	} else { // 如果当前节点不是leader
		// Replica发送txSeq给leader，获取leader的实例执行RPC
		replica, ok := cm.configuration.Replica(leaderID)
		if !ok {
			cm.logger.Infof("[CollectMachine-Collect]: Replica with ID %d was not found!", leaderID)
			return
		}
		cm.logger.Debugf("[CollectMachine-Collect] Replica: node sync currentView: %d, Collect View: %d, Collec View LeaderID: %d", cm.synchronizer.View(), currentView, leaderID)
		replica.Collect(colmsg.CollectTxSeq) // 使用modules.go的Replica接口
	}

}

// OnCollect处理一个collect消息
// RapidFair: baseline 保证collect之后才能执行prepare（pipelined实现时，每次propose都是执行下一个round的prepare）
// 在synchronizer.go中的AdvanceView()中调用，在propose之前执行
func (cm *CollectMachine) OnCollect(col hotstuff.CollectMsg) {
	txSeq := col.CollectTxSeq
	highQcView := cm.synchronizer.LeafBlock().View()
	newView := highQcView + 1
	// currentView := cm.synchronizer.View()
	syncInfo := txSeq.SyncInfo()
	// cm.logger.Infof("Leader OnCollect(%d): view=%d", col.ID, txSeq.View())

	// 发送collectMsg的节点也应该缓存一下
	// 1. 判断消息的view是否和节点本地的view一致
	// 首先判断msg消息中view是否<=当前节点最新block的view+1
	if txSeq.View() <= highQcView {
		return
	}
	// 将收到的collect的交易序列加入到collectedTxSeq保存（内部变量）
	// collect tx的view必须和要新提出的block的view一致
	// collect交易序列=quorum之后，就不会在本地继续存储接收的collect消息了
	if len(cm.collectedTxSeq[newView]) == 0 {
		cm.collectedTxSeq[newView] = make(map[hotstuff.ID]hotstuff.Command)
	}
	if txSeq.View() == newView {
		cm.collectedTxSeq[newView][col.ID] = txSeq.TxSeq()
	}
	// cm.logger.Infof("OnCollect(): after collect tx seq, node id: %d, view: %d, collect from id: %d, collectseq num: %d, quorumsizefair: %d", cm.opts.ID(), newView, col.ID, len(cm.collectedTxSeq[newView]), cm.configuration.QuorumSizeFair())

	// collect交易序列数量不等于quorum时不能执行公平排序
	if len(cm.collectedTxSeq[newView]) != cm.configuration.QuorumSizeFair() {
		// cm.logger.Infof("OnCollect(): should return")
		return
	}
	// 2. 计算公平排序序列（考虑同步 or 异步）
	// odc := cm.FairOrder(hotstuff.TXList(cm.collectedTxSeq[newView]))
	// 补充：计算Themis consensus latency & fair order computation latency
	// if cm.opts.OnlyRunOFO() {
	round := cm.synchronizer.View()
	if round <= 1 {
		cm.totalOrderLatency = 0
	}
	if round <= 10 {
		cm.gStartTime = time.Now()
	} else {
		cm.CalThemisLatency()
	}
	start := time.Now()
	odc := cm.FairOrder(hotstuff.TXList(cm.collectedTxSeq[newView]))
	elapsed := time.Since(start)
	orderLatency := float64(elapsed) / float64(time.Millisecond)
	cm.totalOrderLatency += orderLatency * 2
	if round%20 == 0 {
		avgLatency := cm.totalOrderLatency / float64(round)
		cm.logger.Info("Avg ALL FairOrder Computation time:", avgLatency, "; virtual view: ", round)
	}
	// } else {
	// 	odc := cm.FairOrder(hotstuff.TXList(cm.collectedTxSeq[newView]))
	// }
	// 补充测试：end

	// 3. 公平排序好交易之后，调用FairPropose(cert, cmd)
	// OnCollect也只有leader能处理
	// odc := cm.orderedCommand
	colTxSeq := make(map[hotstuff.ID]hotstuff.Command)
	for k, v := range cm.collectedTxSeq[newView] {
		colTxSeq[k] = v
	}
	// cm.orderedCommand = hotstuff.Command("") // 垃圾回收
	// 当前view的leader收集的交易序列应该延后清空，在本view的区块已经完成vote之后再清空（在下一个view的OnReadyCollect中清空？）
	// cm.collectedTxSeq = make(map[hotstuff.View]map[hotstuff.ID]hotstuff.Command)
	// leader调用fairpropose方法
	cm.consensus.FairPropose(syncInfo, odc, colTxSeq)
}

// RapidFair: baseline 公平排序处理
// CollectMachine.FairOrder主要处理：
// 1. quorum判断
// 2. 交易消息转换Command -> []string
// 3. [][]string矩阵构建
// 4. 调用graph-based公平排序方法计算排序序列 orderedSeq
// 5. 使用orderedSeq重构hotstuff.Command，返回cm.OrderedCommand
func (cm *CollectMachine) FairOrder(cTxSeq hotstuff.TXList) hotstuff.Command {
	cm.mut.Lock()
	defer cm.mut.Unlock()
	if len(cTxSeq) == 0 {
		return hotstuff.Command("")
	}
	// 交易序列数量超过quorum时
	// 转换消息，构建[][]string矩阵
	txList, txinfo := cm.ConstructTxList(cTxSeq)
	// 打印公平排序需要的交易
	/*
		alltxListlen := make([]int, 0)
		for _, v := range txList {
			alltxListlen = append(alltxListlen, len(v))
			cm.logger.Infof("[CollectMachine-FairOrder]: tx seq: %v\n", v)
		}
		cm.logger.Infof("Run [FairOrder]: len(collectedTxSeq)=%d, QuorumSize=%d, nodeNum=%d, gamma=%f, len(txList[])=%v\n", len(cTxSeq), cm.configuration.QuorumSize(), cm.configuration.Len(), cm.opts.ThemisGamma(), alltxListlen)
	*/

	// start := time.Now()
	// finalTxSeq := orderfairness.FairOrder_Themis(txList, cm.configuration.Len(), cm.opts.ThemisGamma())
	// elapsed := time.Since(start)
	// cm.logger.Info("FairOrder Execution time:", elapsed)

	// 公平排序Themis（传入参数控制），输出[]VT类型序列
	finalTxSeq := orderfairness.FairOrder_Themis(txList, cm.configuration.Len(), cm.opts.ThemisGamma())
	cm.logger.Debugf("[Collect-FairOrder]: tx seq len after fair order: %d\n", len(finalTxSeq))
	// 利用公平排序结果重构command，序列化，
	fairBatch := new(clientpb.Batch)
	for _, data := range finalTxSeq {
		c := txinfo[string(data)]
		fairBatch.Commands = append(fairBatch.Commands, c)
	}
	b, err := cm.marshaler.Marshal(fairBatch)
	if err != nil {
		cm.logger.Errorf("[CollectMachine-FairOrder]: Failed to marshal fairBatch: %v", err)
	}
	// 将序列化后的batch数据，再转变为hotstuff.Command类型，并存入cm.orderedCommand
	return hotstuff.Command(b)
}

// 利用txSeq构建[][]string矩阵，并缓存交易的clientID,SequenceNumber
func (cm *CollectMachine) ConstructTxList(txSeq hotstuff.TXList) ([][]string, map[string]*clientpb.Command) {
	txList := make([][]string, 0)
	txinfo := make(map[string]*clientpb.Command)
	// for replicaId, ct := range txSeq {
	for _, ct := range txSeq {
		// 反序列化 hotstuff.Command -> []byte -> []*clientpb.Command
		batch := new(clientpb.Batch)
		err := cm.unmarshaler.Unmarshal([]byte(ct), batch)
		if err != nil {
			cm.logger.Errorf("[ConstructTxList]: Failed to unmarshal batch: %v", err)
			return txList, txinfo
		}
		// 构建二维矩阵[][]string
		txs := make([]string, 0)
		for _, v := range batch.GetCommands() {
			// 用"tx"+clientID+","+SequenceNumber作为交易的id来参与排序
			cmdId := string(hotstuff.NewTxID(v.GetClientID(), v.GetSequenceNumber()))
			txs = append(txs, cmdId)
			// 按交易id缓存交易内容
			if _, ok := txinfo[cmdId]; !ok {
				txinfo[cmdId] = v
			}
		}
		// cm.logger.Infof("[CollectMachine-ConstructTxList]: replica id: %d", replicaId)
		txList = append(txList, txs)
	}
	return txList, txinfo
}

// ReadyCollect: onVote结束后，Leader广播ReadyCollect消息给replicas
func (cm *CollectMachine) ReadyCollect(syncInfo hotstuff.SyncInfo) {
	// 调用ReadyCollect时，leader已经在synchronizer更新了currentView
	v := cm.synchronizer.View()
	cm.logger.Debugf("ReadyCollect from leader, view: %d", v)
	rc := hotstuff.ReadyCollectMsg{
		ID:           cm.opts.ID(),
		ReadyCollect: hotstuff.NewReadyCollect(v, syncInfo),
	}

	cm.configuration.ReadyCollect(rc)
	// leader自己处理消息
	cm.OnReadyCollect(rc)
}

// OnReadyCollect: 处理leader发送的ReadyCollect消息，调用Collect()发txSeq给leader
func (cm *CollectMachine) OnReadyCollect(rc hotstuff.ReadyCollectMsg) {
	// 清空之前所有的view的leader本地缓存的交易序列列表collectedTxSeq
	sync := rc.ReadyCollect.SyncInfo()
	if len(cm.collectedTxSeq) > 0 {
		for k := range cm.collectedTxSeq {
			// 按本地同步的block的view，删除本地收到的最高block之前的所有缓存的collectedTxSeq
			if k <= cm.synchronizer.LeafBlock().View() {
				delete(cm.collectedTxSeq, k)
			}
		}
	}
	// 调用Collect发txSeq给本轮view的leader
	cm.Collect(sync)
}

// 测试整体平均延迟
func (cm *CollectMachine) CalThemisLatency() {
	duration := time.Since(cm.gStartTime)
	roundLatency := float64(duration) / float64(time.Millisecond)
	round := cm.synchronizer.View()
	// cs.logger.Info("Consensus round latency:", roundLatency, "; virtual view: ", round)
	cm.gStartTime = time.Now()
	cm.totalLatency += roundLatency
	if round%20 == 0 {
		avgLatency := cm.totalLatency / float64(round-10)
		cm.logger.Info("Themis avg latency:", avgLatency, "; virtual view: ", round)
	}
}
