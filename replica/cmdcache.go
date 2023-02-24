package replica

import (
	"container/list"
	"context"
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

	mut           sync.Mutex
	c             chan struct{}
	batchSize     int
	serialNumbers map[uint32]uint64 // highest proposed serial number per client ID（存储包括所有client的最高已提交交易序列）
	cache         list.List         // 使用链表list缓存收到的交易command，在哪里初始化的？
	marshaler     proto.MarshalOptions
	unmarshaler   proto.UnmarshalOptions

	proposedCache     map[uint32]map[uint64]int // RapidFair:baseline 存储对所有已提交交易的缓存（无序）
	currentBatchCache []*clientpb.Command       // RapidFair:baseline 存储对当前读取的所有txBatch的缓存
}

func newCmdCache(batchSize int) *cmdCache {
	return &cmdCache{
		c:                 make(chan struct{}),
		batchSize:         batchSize,
		serialNumbers:     make(map[uint32]uint64),
		proposedCache:     make(map[uint32]map[uint64]int),
		currentBatchCache: make([]*clientpb.Command, batchSize),
		marshaler:         proto.MarshalOptions{Deterministic: true},
		unmarshaler:       proto.UnmarshalOptions{DiscardUnknown: true},
	}
}

// InitModule gives the module access to the other modules.
func (c *cmdCache) InitModule(mods *modules.Core) {
	mods.Get(&c.logger)
}

func (c *cmdCache) addCommand(cmd *clientpb.Command) {
	c.mut.Lock()
	defer c.mut.Unlock()
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
	for i := 0; i < c.batchSize; i++ {
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
	if len(batch.Commands) == 0 {
		goto awaitBatch
	}

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
	// c.cache是一个list，这里只读但是不删除，在addCommand中直接增加tx
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
		// 判断cmd的seqnum是否已经提交，如果已经提交，则不会加入这个交易进入batch
		if _, ok := c.proposedCache[cmd.GetClientID()][cmd.GetSequenceNumber()]; ok {
			i--
			continue
		}
		batch.Commands = append(batch.Commands, cmd)
		c.currentBatchCache = append(c.currentBatchCache, cmd)
	}

	// if we still got no (new) commands, try to wait again
	// 如果没有command则会再去等待（RapidFair: baseline中，如果不到batchsize，也会继续等待）
	if len(batch.Commands) < c.batchSize {
		goto awaitBatch
	}

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

// Accept returns true if the replica can accept the batch.
func (c *cmdCache) Accept(cmd hotstuff.Command) bool {
	batch := new(clientpb.Batch)
	err := c.unmarshaler.Unmarshal([]byte(cmd), batch)
	if err != nil {
		c.logger.Errorf("Failed to unmarshal batch: %v", err)
		return false
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	for _, cmd := range batch.GetCommands() {
		if serialNo := c.serialNumbers[cmd.GetClientID()]; serialNo >= cmd.GetSequenceNumber() {
			// command is too old, can't accept
			return false
		}
	}

	return true
}

// Proposed updates the serial numbers such that we will not accept the given batch again.
// RapidFair: baseline 修改对proposed交易缓存的修改
func (c *cmdCache) Proposed(cmd hotstuff.Command) {
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
		if c.proposedCache[cmd.GetClientID()] == nil {
			c.proposedCache[cmd.GetClientID()] = make(map[uint64]int)
		}
		c.proposedCache[cmd.GetClientID()][cmd.GetSequenceNumber()] = 1

		if serialNo := c.serialNumbers[cmd.GetClientID()]; serialNo < cmd.GetSequenceNumber() {
			c.serialNumbers[cmd.GetClientID()] = cmd.GetSequenceNumber()
		}
	}

	// RapidFair: baseline 将没有提交成功的交易应该按顺序再放回c.cache
	// currentBatchCache >= proposal
	var remainedTx []*clientpb.Command
	for _, v := range c.currentBatchCache {
		if _, ok := c.proposedCache[v.GetClientID()][v.GetSequenceNumber()]; !ok {
			remainedTx = append(remainedTx, v)
		}
	}
	if len(remainedTx) > 0 {
		for i := len(remainedTx) - 1; i >= 0; i-- {
			c.cache.PushFront(remainedTx[i])
		}
	}
}

// RapidFair: baseline 获取当前最高的交易序列号
func (c *cmdCache) GetHighProposedSeqNum() uint64 {
	highSeqNum := 0
	for _, v := range c.proposedCache {
		highSeqNum += len(v)
	}
	return uint64(highSeqNum)
}

var _ modules.Acceptor = (*cmdCache)(nil)
