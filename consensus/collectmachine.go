package consensus

import (
	"sync"

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
	blockChain     modules.BlockChain
	consensus      modules.Consensus
	configuration  modules.Configuration
	commandQueue   modules.CommandQueue
	eventLoop      *eventloop.EventLoop
	leaderRotation modules.LeaderRotation
	logger         logging.Logger
	synchronizer   modules.Synchronizer
	opts           *modules.Options

	mut            sync.Mutex
	collectedTxSeq map[hotstuff.ID]hotstuff.Command    // 存储收集的交易序列列表
	orderedCommand hotstuff.Command                    // 存储公平排序后的交易序列
	syncCache      map[hotstuff.View]hotstuff.SyncInfo // 缓存viewMsg提供的同步信息，用于调用consensus.Propose，replica.NewView
	marshaler      proto.MarshalOptions                // 序列化（来自proto）
	unmarshaler    proto.UnmarshalOptions              // 反序列化
}

// 创建一个新NewCollectMachine
// 不一定需要构建一个interface，也不一定要注册模块？说起来应该只有存在不同版本的相似算法时需要注册模块进行指定配置
// func NewCollectMachine() *CollectMachine {
func NewCollectMachine() modules.Collector {
	return &CollectMachine{
		collectedTxSeq: make(map[hotstuff.ID]hotstuff.Command),
		syncCache:      make(map[hotstuff.View]hotstuff.SyncInfo),
		marshaler:      proto.MarshalOptions{Deterministic: true},
		unmarshaler:    proto.UnmarshalOptions{DiscardUnknown: true},
	}
}

// 初始化模块，注册处理函数
func (cm *CollectMachine) InitModule(mods *modules.Core) {
	mods.Get(
		&cm.blockChain,
		&cm.consensus,
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
}

// collection流程
// 1. replica从本地交易列表中提取batch-size的交易序列txSeq
// 2. replica将txSeq包装成消息CollectTxSeq
// 3. replica节点将CollectTxSeq发送给当前View的leader节点
func (cm *CollectMachine) Collect(syncInfo hotstuff.SyncInfo) {
	// 缓存来自hotstuff.NewViewMsg的同步信息
	cm.syncCache[cm.synchronizer.View()] = syncInfo
	// 研究cmdcache.go实现的CommandQueue的Get()方法，如何取出batchtx
	// 设计新的GetTxBatch()，使得在获取batchtx进行collect的时候不会从cache中删除，只有收到proposal并通过后才从cache里删除
	cmd, ok := cm.commandQueue.GetTxBatch(cm.synchronizer.ViewContext())
	if !ok {
		cm.logger.Debug("[CollectMachine-Collect] Collect: No command")
		return
	}
	// var colmsg hotstuff.CollectMsg
	// 将batchsize大小的txSeq包装成消息CollectTxSeq
	colmsg := hotstuff.CollectMsg{
		ID: cm.opts.ID(),
		CollectTxSeq: hotstuff.NewCollectTxSeq(
			cm.synchronizer.View(), // Collect方法调用时可以确保View已经更新
			cmd,
		),
	}
	// 远程调用leader的Collect方法，去Oncollect处理
	// 获取当前View的leader
	leaderID := cm.leaderRotation.GetLeader(cm.synchronizer.View())
	if leaderID == cm.opts.ID() { // 如果当前节点是leader
		// 直接增加事件，交给oncollect()处理
		cm.eventLoop.AddEvent(colmsg)
	} else { // 如果当前节点不是leader
		// Replica发送txSeq给leader，获取leader的实例执行RPC
		replica, ok := cm.configuration.Replica(leaderID)
		if !ok {
			cm.logger.Warnf("[CollectMachine-Collect]: Replica with ID %d was not found!", leaderID)
			return
		}
		replica.Collect(colmsg.CollectTxSeq) // 使用modules.go的Replica接口
	}

}

// OnCollect处理一个collect消息
// RapidFair: baseline 保证collect之后才能执行prepare（pipelined实现时，每次propose都是执行下一个round的prepare）
// 在synchronizer.go中的AdvanceView()中调用，在propose之前执行
func (cm *CollectMachine) OnCollect(col hotstuff.CollectMsg) {
	txSeq := col.CollectTxSeq
	cm.logger.Debugf("OnCollect(%d): view=%d", col.ID, txSeq.View())
	// 发送collectMsg的节点也应该缓存一下
	// 1. 判断消息的view是否和节点本地的view一致
	// 首先判断msg消息中view是否<=当前节点的block的view+1
	if txSeq.View() <= cm.synchronizer.LeafBlock().View() {
		return
	}
	// 将收到的collect的交易序列加入到collectedTxSeq保存（内部变量）
	if len(cm.collectedTxSeq) < cm.configuration.QuorumSize() {
		cm.collectedTxSeq[col.ID] = txSeq.TxSeq()
	}
	// 2. 计算公平排序序列（考虑同步 or 异步）
	cm.FairOrder(txSeq.TxSeq())
	// 3. 公平排序好交易之后，调用FairPropose(cert, cmd)
	if syncInfo, ok := cm.syncCache[cm.synchronizer.View()]; ok {
		leaderID := cm.leaderRotation.GetLeader(cm.synchronizer.View())
		if leaderID == cm.opts.ID() { // 如果当前节点是leader
			odc := cm.orderedCommand
			colTxSeq := make(map[hotstuff.ID]hotstuff.Command)
			for k, v := range cm.collectedTxSeq {
				colTxSeq[k] = v
			}
			cm.orderedCommand = hotstuff.Command("") // 垃圾回收
			cm.collectedTxSeq = make(map[hotstuff.ID]hotstuff.Command)
			cm.consensus.FairPropose(syncInfo, odc, colTxSeq)
		} else {
			if replica, ok := cm.configuration.Replica(leaderID); ok {
				replica.NewView(syncInfo)
			} else {
				cm.logger.Warnf("[CollectMachine-OnCollect]: Replica with ID %d was not found!", leaderID)
				return
			}
		}
	} else {
		cm.logger.Info("[CollectMachine] could not get corret syncInfo")
	}

}

// RapidFair: baseline 公平排序处理
// CollectMachine.FairOrder主要处理：
// 1. quorum判断
// 2. 交易消息转换Command -> []string
// 3. [][]string矩阵构建
// 4. 调用graph-based公平排序方法计算排序序列 orderedSeq
// 5. 使用orderedSeq重构hotstuff.Command，写入cm.OrderedCommand
func (cm *CollectMachine) FairOrder(cmd hotstuff.Command) {
	cm.mut.Lock()
	defer cm.mut.Unlock()
	// 如果收到的交易序列数量小于quorum
	if len(cm.collectedTxSeq) < cm.configuration.QuorumSize() {
		return
	}
	// 交易序列数量超过quorum时
	// 转换消息，构建[][]string矩阵
	txList, txInfo := cm.ConstructTxList(cm.collectedTxSeq)
	// 公平排序Themis（传入参数控制），输出[]VT类型序列
	finalTxSeq := orderfairness.FairOrder_Themis(txList, cm.configuration.Len(), cm.opts.ThemisGamma())
	// 利用公平排序结果重构command，序列化，并存入cm.orderedCommand
	highSeqNum := cm.commandQueue.GetHighProposedSeqNum()
	fairBatch := new(clientpb.Batch)
	for i, data := range finalTxSeq {
		c := &clientpb.Command{
			ClientID:       txInfo[string(data)],
			SequenceNumber: highSeqNum + uint64(i) + 1,
			Data:           []byte(string(data)),
		}
		fairBatch.Commands = append(fairBatch.Commands, c)
	}
	b, err := cm.marshaler.Marshal(fairBatch)
	if err != nil {
		cm.logger.Errorf("[CollectMachine-FairOrder]: Failed to marshal fairBatch: %v", err)
		return
	}
	// 将序列化后的batch数据，再转变为hotstuff.Command类型
	cm.orderedCommand = hotstuff.Command(b)
}

// 利用txSeq构建[][]string矩阵，并缓存交易的clientID
func (cm *CollectMachine) ConstructTxList(txSeq map[hotstuff.ID]hotstuff.Command) ([][]string, map[string]uint32) {
	txList := make([][]string, 0)
	txInfo := make(map[string]uint32)
	for _, ct := range txSeq {
		// 反序列化 hotstuff.Command -> []byte -> []*clientpb.Command
		batch := new(clientpb.Batch)
		err := cm.unmarshaler.Unmarshal([]byte(ct), batch)
		if err != nil {
			cm.logger.Errorf("[CollectMachine-FairOrder]: Failed to unmarshal batch: %v", err)
			return txList, txInfo
		}
		// 构建二维矩阵[][]string
		txs := make([]string, 0)
		for _, v := range batch.GetCommands() {
			// txList[i] = append(txList[i], string(v.GetData()))
			txs = append(txs, string(v.GetData()))
			// 缓存一下每个交易的clientID
			if _, ok := txInfo[string(v.GetData())]; !ok {
				txInfo[string(v.GetData())] = v.GetClientID()
			}
		}
		txList = append(txList, txs)
	}
	return txList, txInfo
}
