package metrics

import (
	"time"

	"github.com/relab/hotstuff"

	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/metrics/types"
	"github.com/relab/hotstuff/modules"
	"google.golang.org/protobuf/types/known/durationpb"
)

func init() {
	RegisterReplicaMetric("throughput", func() any {
		return &Throughput{}
	})
}

// Throughput measures throughput in commits per second, and commands per second.
type Throughput struct {
	metricsLogger Logger
	opts          *modules.Options

	commitCount  uint64
	commandCount uint64
}

// InitModule gives the module access to the other modules.
func (t *Throughput) InitModule(mods *modules.Core) {
	var (
		eventLoop *eventloop.EventLoop
		logger    logging.Logger
	)

	mods.Get(
		&t.metricsLogger,
		&t.opts,
		&eventLoop,
		&logger,
	)
	// 注册处理CommitEvent的函数，CommitEvent在./replica/clientsrv.go的Exec()中被加入
	eventLoop.RegisterHandler(hotstuff.CommitEvent{}, func(event any) {
		commitEvent := event.(hotstuff.CommitEvent)
		t.recordCommit(commitEvent.Commands)
	})

	// tps注册观察者函数
	eventLoop.RegisterObserver(types.TickEvent{}, func(event any) {
		t.tick(event.(types.TickEvent))
	})

	logger.Info("Throughput metric enabled")
}

func (t *Throughput) recordCommit(commands int) {
	t.commitCount++
	t.commandCount += uint64(commands)
}

func (t *Throughput) tick(tick types.TickEvent) {
	now := time.Now()
	event := &types.ThroughputMeasurement{
		Event:    types.NewReplicaEvent(uint32(t.opts.ID()), now),
		Commits:  t.commitCount,
		Commands: t.commandCount,
		Duration: durationpb.New(now.Sub(tick.LastTick)),
	}
	// 调用./metrics/datalogger.go 的func (dl *jsonLogger) Log(msg proto.Message)方法
	// 将消息写入json文件？
	t.metricsLogger.Log(event)
	// reset count for next tick
	t.commandCount = 0
	t.commitCount = 0
}
