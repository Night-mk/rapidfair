package replica

import (
	"container/list"
	"context"
	"crypto/sha256"
	"sync"

	"github.com/relab/hotstuff"

	"github.com/relab/hotstuff/internal/proto/clientpb"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
	"google.golang.org/protobuf/proto"
)

// 在./replica/clientsrv.go中使用
// 实现了CommandQueue接口
type cmdCache struct {
	logger logging.Logger
	opts   *modules.Options // RapidFair: 控制使用公平排序

	mut           sync.Mutex
	c             chan struct{}
	batchSize     int
	serialNumbers map[uint32]uint64 // highest proposed serial number per client ID（存储包括所有client的最高已提交交易序列）
	cache         list.List         // 使用链表list缓存收到的交易command，在哪里初始化的？
	marshaler     proto.MarshalOptions
	unmarshaler   proto.UnmarshalOptions

	proposedCache     map[hotstuff.TxID]int               // RapidFair:baseline 存储对所有已提交交易的缓存（无序）
	currentBatchCache map[hotstuff.TxID]*clientpb.Command // RapidFair:baseline 存储对当前读取的所有txBatch的缓存
	currentBatchTxId  []hotstuff.TxID                     // 按顺序存储加入到txBatch的交易
	fragmentTxCache   map[hotstuff.TxID]int               // RapidFair: 存储当前已经构建fragment但是没有构建QC，且没有提交到block的交易集合
	highFragVirView   hotstuff.View                       // 记录当前最高的Fragment的virView
	fragList          list.List                           // 缓存写入cmdcache的Fragment
	canGetFrag        chan struct{}                       // 用channel控制是否能从cmdcahe中读取Fragment
}

func newCmdCache(batchSize int) *cmdCache {
	return &cmdCache{
		c:                 make(chan struct{}),
		batchSize:         batchSize,
		serialNumbers:     make(map[uint32]uint64),
		proposedCache:     make(map[hotstuff.TxID]int),
		currentBatchCache: make(map[hotstuff.TxID]*clientpb.Command),
		currentBatchTxId:  make([]hotstuff.TxID, 0),
		fragmentTxCache:   make(map[hotstuff.TxID]int),
		highFragVirView:   0,
		canGetFrag:        make(chan struct{}),
		marshaler:         proto.MarshalOptions{Deterministic: true},
		unmarshaler:       proto.UnmarshalOptions{DiscardUnknown: true},
	}
}

// InitModule gives the module access to the other modules.
func (c *cmdCache) InitModule(mods *modules.Core) {
	mods.Get(
		&c.logger,
		&c.opts,
	)
}

// 向cmdcache增加从client接收到的tx
// 判断client发来的交易序列号是否小于已经提交的交易的序列号（正常情况下一定是大于的，即cmd.GetSequenceNumber()>proposed serialNo）
func (c *cmdCache) addCommand(cmd *clientpb.Command) {
	c.mut.Lock()
	defer c.mut.Unlock()
	// 判断client发来的tx的序列号是否小于该client已经提交的最高的交易序列号
	if serialNo := c.serialNumbers[cmd.GetClientID()]; serialNo >= cmd.GetSequenceNumber() {
		// command is too old
		return
	}
	c.cache.PushBack(cmd) // 输入的其实就是*clientpb.Command类型的数据
	if c.cache.Len() >= c.batchSize {
		// notify Get that we are ready to send a new batch.
		// 通知Get()方法可以发送new batch
		select {
		case c.c <- struct{}{}:
		default:
		}
	}
}

// CommandQueue的Get方法在这里实现
// 这里算是节点的内存？
// Get returns a batch of commands to propose.
func (c *cmdCache) Get(ctx context.Context) (cmd hotstuff.Command, ok bool) {
	// 初始化的batch类型: 由client.proto定义
	/*type Batch struct {
		state         protoimpl.MessageState
		sizeCache     protoimpl.SizeCache
		unknownFields protoimpl.UnknownFields
		Commands []*Command `protobuf:"bytes,1,rep,name=Commands,proto3" json:"Commands,omitempty"`
	}*/
	batch := new(clientpb.Batch)
	// c.logger.Infof("default batch size: %d", c.batchSize) // default batch size=1

	c.mut.Lock()
awaitBatch:
	// wait until we can send a new batch.
	for c.cache.Len() <= c.batchSize {
		c.mut.Unlock()
		select { // select 能够让 Goroutine 同时等待多个 Channel 可读或者可写
		// 这里同时等待两个事件：
		case <-c.c: // 如果通道<-c.c收到内容 通知Get()方法可以发送new batch
		case <-ctx.Done(): // 如果context结束，则不执行这个方法了（用于控制在一个view里读取txbatch方法的同步）
			return
		}
		c.mut.Lock()
	}

	// Get the batch. Note that we may not be able to fill the batch, but that should be fine as long as we can send
	// at least one command.
	// c.cache是一个list，这里的读取方式是读一个删一个，在addCommand中直接增加tx
	// for i := 0; i < c.batchSize; i++ {
	for i := 0; i < c.batchSize-len(batch.Commands); i++ {
		elem := c.cache.Front()
		if elem == nil {
			break
		}
		c.cache.Remove(elem)                  // 从list里删除tx
		cmd := elem.Value.(*clientpb.Command) // 获取elem list的值，并使用断言方式判断是否能转换为*clientpb.Command类型
		// 获取客户端ID的最高proposed序列号
		if serialNo := c.serialNumbers[cmd.GetClientID()]; serialNo >= cmd.GetSequenceNumber() {
			// command is too old
			// 如果本地最高的已提交的交易序列号>=本地cache中接收到的交易的序列号，则删除这条读入的交易（这里默认低序列号的交易已经被proposed）
			i--
			continue
		}
		batch.Commands = append(batch.Commands, cmd)
	}

	// if we still got no (new) commands, try to wait again
	// 如果没有command则会再去等待
	// if len(batch.Commands) == 0 {
	// 	goto awaitBatch
	// }
	// hotstuff baseline修改，让每个block都保证至少batchsize大小
	if len(batch.Commands) < c.batchSize {
		goto awaitBatch
	}
	// c.logger.Infof("Setted BatchSize: %d", c.batchSize)
	// c.logger.Infof("BatchSize of get: %d", len(batch.Commands))

	defer c.mut.Unlock()

	// otherwise, we should have at least one command
	b, err := c.marshaler.Marshal(batch)
	if err != nil {
		c.logger.Errorf("Failed to marshal batch: %v", err)
		return "", false
	}

	// 将序列化后的batch数据变为hotstuff.Command类型
	cmd = hotstuff.Command(b)
	return cmd, true
}

// RapidFair: baseline 设计新的交易读取方法GetTxBatch()
// 使得在获取batchtx进行collect的时候不会从cache中删除，只有收到proposal并通过后才从cache里删除
func (c *cmdCache) GetTxBatch(ctx context.Context) (cmd hotstuff.Command, ok bool) {
	// 使用new对clientpb.Batch进行实例化
	batch := new(clientpb.Batch)
	c.mut.Lock()
awaitBatch:
	// wait until we can send a new batch.
	for c.cache.Len() <= c.batchSize {
		c.mut.Unlock()
		select { // select 能够让 Goroutine 同时等待多个 Channel 可读或者可写
		// 这里同时等待两个事件：
		case <-c.c: // 如果通道<-c.c收到内容 通知Get()方法可以发送new batch
		case <-ctx.Done(): // 如果context结束，则不执行这个方法了（用于控制在一个view里读取txbatch方法的同步）
			return
		}
		c.mut.Lock()
	}

	// Get the batch. Note that we may not be able to fill the batch, but that should be fine as long as we can send at least one command.
	// c.cache是一个list，在addCommand中接收client发来的tx
	// 这个方法中必须持续读取batchsize长度的交易序列
	for i := 0; i < c.batchSize-len(batch.Commands); i++ {
		elem := c.cache.Front()
		if elem == nil {
			break
		}
		c.cache.Remove(elem)                  // 从list里删除tx
		cmd := elem.Value.(*clientpb.Command) // 获取elem list的值，并使用断言方式判断是否能转换为*clientpb.Command类型
		/* message Command {
			uint32 ClientID = 1;
			uint64 SequenceNumber = 2;
			bytes Data = 3;
		}*/
		// 判断什么交易可以加入到当前的Batch
		// 1. 判断cmd的seqnum是否已经提交，如果已经提交，则不会加入这个交易进入batch
		txId := hotstuff.NewTxID(cmd.GetClientID(), cmd.GetSequenceNumber())
		if _, ok := c.proposedCache[txId]; ok {
			i--
			continue
		}
		// 2. RapidFair额外判断cmd是否提交到fragment但是还没提交到Block，这种交易也不能加入到当前batch
		if _, ok := c.fragmentTxCache[txId]; ok {
			i--
			continue
		}
		// 将没有重复的交易cmd加入到batch中
		// RapidFair优化：交易的data field只用计算hash，之后加入batch
		// if c.opts.UseRapidFair() || c.opts.UseFairOrder() { // Themis优化后，data进行hash之后传输
		if c.opts.UseFairOrder() { // Themis优化后，data进行hash之后传输
			// if c.opts.UseRapidFair() { // Themis优化前
			dataHash := sha256.Sum256(cmd.Data)
			cmd.Data = dataHash[:]
		}
		if c.opts.UseRapidFair() { // RapidFair再次优化，cmd只记录ID，不记录Data
			cmd.Data = []byte{}
		}
		batch.Commands = append(batch.Commands, cmd)
		c.currentBatchCache[txId] = cmd
		c.currentBatchTxId = append(c.currentBatchTxId, txId) // 记录txId的交易的索引
	}

	// if we still got no (new) commands, try to wait again
	// 如果没有command则会再去等待（RapidFair: baseline中，如果不到batchsize，也会继续等待）
	if len(batch.Commands) < c.batchSize {
		goto awaitBatch
	}
	// c.logger.Infof("[GetTxBatch]: BatchSize of get: %d", len(batch.Commands))

	defer c.mut.Unlock()

	// otherwise, we should have at least one command
	b, err := c.marshaler.Marshal(batch)
	if err != nil {
		c.logger.Errorf("Failed to marshal batch: %v", err)
		return "", false
	}
	// 将序列化后的batch数据，再转变为hotstuff.Command类型
	cmd = hotstuff.Command(b)
	return cmd, true
}

// Accept returns true if the replica can accept the batch. 只在onPropose中调用
// 判断replica是否会接受提出的block中的交易，一般block中cmd的序列号要大于client已经提交的最高序列号(serialNumbers[clientID])
// RapidFair问题就在这里：
func (c *cmdCache) Accept(cmd hotstuff.Command) bool {
	batch := new(clientpb.Batch)
	err := c.unmarshaler.Unmarshal([]byte(cmd), batch)
	if err != nil {
		c.logger.Errorf("Failed to unmarshal batch: %v", err)
		return false
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	// RapidFair: baseline 原本hotstuff这里判断序列号可能存在问题，因为可能存在一些低序列号的交易无法在当前block提交，并延后到下一个或几个block中才能提交
	// RapidFair: baseline 公平排序里暂时先放弃判断
	if !c.opts.UseFairOrder() && !c.opts.UseRapidFair() {
		for _, cmd := range batch.GetCommands() {
			if serialNo := c.serialNumbers[cmd.GetClientID()]; serialNo >= cmd.GetSequenceNumber() {
				// command is too old, can't accept
				return false
			}
		}
	}

	return true
}

// Proposed updates the serial numbers such that we will not accept the given batch again.
// RapidFair: baseline 修改对proposed交易缓存的修改
// hotstuff: 输入已经提出的block的交易序列，更新client提交的最高交易序列号
func (c *cmdCache) Proposed(cmd hotstuff.Command) {
	// c.logger.Infof("==============call cmdcahe proposed()==============")
	batch := new(clientpb.Batch)
	err := c.unmarshaler.Unmarshal([]byte(cmd), batch)
	if err != nil {
		c.logger.Errorf("Failed to unmarshal batch: %v", err)
		return
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	for _, cmd := range batch.GetCommands() {
		// RapidFair: baseline 将已提交的tx序列号保存在cmdcache本地
		// 在构造block的时候没有重构交易的SequenceNumber
		txId := hotstuff.NewTxID(cmd.GetClientID(), cmd.GetSequenceNumber())
		c.proposedCache[txId] = 1
		// 从currentBatchCache中删除已经提交的交易
		delete(c.currentBatchCache, txId)
		// RapidFair END

		// 记录每个client提出的并且包含在proposal中的交易(cmd)的最高序列号
		if serialNo := c.serialNumbers[cmd.GetClientID()]; serialNo < cmd.GetSequenceNumber() {
			c.serialNumbers[cmd.GetClientID()] = cmd.GetSequenceNumber()
		}
	}

	// RapidFair: baseline 将没有提交成功的交易应该按顺序再放回c.cache
	if len(c.currentBatchCache) > 0 {
		c.logger.Infof("==============Need reinput the transactions==============")
		// 对在batch中但是没有在block.data中的交易，按从后往前的顺序从前面加入回cache中
		for i := len(c.currentBatchTxId) - 1; i >= 0; i-- {
			if _, ok := c.currentBatchCache[c.currentBatchTxId[i]]; ok {
				c.cache.PushFront(c.currentBatchCache[c.currentBatchTxId[i]])
			}
		}
		// 重置currentBatchCache
		c.currentBatchCache = make(map[hotstuff.TxID]*clientpb.Command, 0)
		c.currentBatchTxId = make([]hotstuff.TxID, 0)
	}
}

// RapidFair: optimisticfairorder中，replica将当前提交到Fragment的交易加入到fragmentTxCache
// view用于更新该fragment的最高提交view
func (c *cmdCache) ProposedFragment(virView hotstuff.View, cmd hotstuff.Command) {
	batch := new(clientpb.Batch)
	err := c.unmarshaler.Unmarshal([]byte(cmd), batch)
	if err != nil {
		c.logger.Errorf("Failed to unmarshal batch: %v", err)
		return
	}

	c.mut.Lock()
	defer c.mut.Unlock()
	/*
		curKeys := make([]hotstuff.TxID, 0)
		fragKeys := make([]hotstuff.TxID, 0)
		if len(c.currentBatchCache) > 0 {
			for k := range c.currentBatchCache {
				curKeys = append(curKeys, k)
			}
		}
	*/
	for _, cmd := range batch.GetCommands() {
		txId := hotstuff.NewTxID(cmd.GetClientID(), cmd.GetSequenceNumber())
		c.fragmentTxCache[txId] = 1
		delete(c.currentBatchCache, txId)
		// fragKeys = append(fragKeys, txId)
	}
	// 更新提交的fragment的最新的virView
	// c.highFragVirView = virView

	// fmt.Println("currentBatchCache: ", curKeys)
	// fmt.Println("ProposedFragment: ", fragKeys)
	// c.logger.Infof("==============[ProposedFragment]: Fragment command len: %d, currentBatchCache len: %d==============", len(batch.GetCommands()), len(c.currentBatchCache))

	// RapidFair: 将没有提交成功的交易应该按顺序再放回c.cache
	if len(c.currentBatchCache) > 0 {
		c.logger.Infof("==============[ProposedFragment]: Need reinput the transactions==============")
		// 对在batch中但是没有在block.data中的交易，按从后往前的顺序从前面加入回cache中
		for i := len(c.currentBatchTxId) - 1; i >= 0; i-- {
			if _, ok := c.currentBatchCache[c.currentBatchTxId[i]]; ok {
				c.cache.PushFront(c.currentBatchCache[c.currentBatchTxId[i]])
			}
		}
		// 重置currentBatchCache
		c.currentBatchCache = make(map[hotstuff.TxID]*clientpb.Command, 0)
		c.currentBatchTxId = make([]hotstuff.TxID, 0)
	}
}

// RapidFair: 共识节点对提交到Block中的交易，更新proposedCache
func (c *cmdCache) ProposedBlock(frag []hotstuff.FragmentData, cmd hotstuff.Command) {
	batch := new(clientpb.Batch)
	err := c.unmarshaler.Unmarshal([]byte(cmd), batch)
	if err != nil {
		c.logger.Errorf("Failed to unmarshal batch: %v", err)
		return
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	// c.logger.Infof("==============[ProposedBlock]: QC Block command len: %d==============", len(batch.GetCommands()))
	for _, cmd := range batch.GetCommands() {
		txId := hotstuff.NewTxID(cmd.GetClientID(), cmd.GetSequenceNumber())
		c.proposedCache[txId] = 1
		// 提出一个新block之后，就应该删除对应的fragment缓存中的交易
		delete(c.fragmentTxCache, txId)
	}
	// 提出新block后，replica也应该从fragList中删除已经提交的fragData对应的fragment
	if len(frag) > 0 {
		lastVirView := frag[len(frag)-1].VirView()
		// 根据block中提交的fragment的virView决定是否要更新highFragVirView
		if c.highFragVirView < lastVirView {
			c.highFragVirView = lastVirView
		}
		for {
			elem := c.fragList.Front()
			if elem == nil {
				break
			}
			f := elem.Value.(*hotstuff.Fragment)
			if f.VirView() <= lastVirView {
				c.fragList.Remove(elem)
			} else { // 如果cmdcache中fragList的fragment的virView超过block提出的fragment的virView，则退出删除循环
				break
			}
		}
	}
}

// 判断提出block的fragment是否能被接受accept，如果
// func (c *cmdCache) AcceptBlock(frag []hotstuff.FragmentData) bool{
// 	c.mut.Lock()
// 	defer c.mut.Unlock()

// 	if len(frag)
// }

// RapidFair: AddFragment用于optimisticfairorder将Fragment加入fragList链表
func (c *cmdCache) AddFragment(fragment *hotstuff.Fragment) {
	c.mut.Lock()
	defer c.mut.Unlock()

	// c.logger.Infof("[AddFragment] highvirView: %d, fragment virView: %d", c.highFragVirView, fragment.VirView())
	// 该virtual view的fragment的已经提交过了 (应该什么时候确定？)
	if fragment.VirView() != 0 {
		if c.highFragVirView >= fragment.VirView() {
			return
		}
	}
	// 将fragment写入list
	c.fragList.PushBack(fragment)
	// 这里就直接更新highFragVirView
	c.highFragVirView = fragment.VirView()

	if c.fragList.Len() > 0 {
		// c.logger.Infof("[AddFragment]: Have added fragment view=%d, fragList len=%d, in nodeID: %d", fragment.VirView(), c.fragList.Len(), c.opts.ID())
		// 通知GetFragment方法已经写入了新Fragment,可以开始读取到block中
		select {
		case c.canGetFrag <- struct{}{}:
		default:
		}
	}
}

// RapidFair: 共识模块中leader从FragmentChain中记录的Fragments中获取有序交易
// 这里ctx是consensus的上下文
func (c *cmdCache) GetFragment(ctx context.Context) (fragments []*hotstuff.Fragment, ok bool) {
	// c.logger.Infof("[GetFragment]: nodeID: %d", c.opts.ID())
	c.mut.Lock()
	// fragments := make([]*hotstuff.Fragment, 0)
	// 等待条件，至少等待1个fragment，fragment数量不超过batchSize
	for c.fragList.Len() <= 0 {
		// c.logger.Infof("[GetFragment]: need wait nodeID: %d", c.opts.ID())
		c.mut.Unlock()
		select { // select 能够让 Goroutine 同时等待多个 Channel 可读或者可写
		case <-c.canGetFrag:
		case <-ctx.Done():
			c.logger.Infof("[GetFragment]: ctx done: %d", c.opts.ID())
			return
		}
		c.mut.Lock()
	}
	// c.logger.Infof("[GetFragment]: Have Fragment nodeID: %d", c.opts.ID())
	// 如果只有一个fragment，则可以直接返回fragment的交易部分
	// 从fragment中读取交易并加入到batch中
	for i := 0; i < c.batchSize; i++ {
		elem := c.fragList.Front()
		if elem == nil {
			break
		}
		c.fragList.Remove(elem) // 从list里删除fragment
		f := elem.Value.(*hotstuff.Fragment)

		fragments = append(fragments, f)

		// 打印一下所有的fragment的OrderedTx看一下
		/*
			txs := f.OrderedTx()
			batch := new(clientpb.Batch)
			err := c.unmarshaler.Unmarshal([]byte(txs), batch)
			if err != nil {
				c.logger.Infof("[GetFragment]: Failed to unmarshal batch: %v", err)
			}
			txsId := make([]hotstuff.TxID, 0)
			for _, cmd := range batch.GetCommands() {
				txId := hotstuff.NewTxID(cmd.GetClientID(), cmd.GetSequenceNumber())
				txsId = append(txsId, txId)
			}
			c.logger.Infof("Get fragment txs: %v", txsId)
		*/
	}
	// c.logger.Infof("[GetFragment]: current fragList len: %d", c.fragList.Len())
	// c.logger.Infof("[GetFragment]: fragment len: %d", len(fragments))

	defer c.mut.Unlock()

	return fragments, true
}

var _ modules.Acceptor = (*cmdCache)(nil)
