// Package replica provides the required code for starting and running a replica and handling client requests.
package replica

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/modules"

	"github.com/relab/gorums"
	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/backend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
)

// cmdID is a unique identifier for a command
type cmdID struct {
	clientID    uint32
	sequenceNum uint64
}

// Config configures a replica.
type Config struct {
	// The id of the replica.
	ID hotstuff.ID
	// The private key of the replica.
	PrivateKey hotstuff.PrivateKey
	// Controls whether TLS is used.
	TLS bool
	// The TLS certificate.
	Certificate *tls.Certificate
	// The root certificates trusted by the replica.
	RootCAs *x509.CertPool
	// The number of client commands that should be batched together in a block.
	BatchSize uint32
	// Options for the client server.
	ClientServerOptions []gorums.ServerOption
	// Options for the replica server.
	ReplicaServerOptions []gorums.ServerOption
	// Options for the replica manager.
	ManagerOptions []gorums.ManagerOption
}

// Replica is a participant in the consensus protocol.
// 在共识里Replica既要做client也要做server
type Replica struct {
	clientSrv *clientSrv
	cfg       *backend.Config
	hsSrv     *backend.Server
	hs        *modules.Core

	execHandlers map[cmdID]func(*emptypb.Empty, error)
	cancel       context.CancelFunc
	done         chan struct{}

	// RapidFair: 增加对fairSynchronizer的上下文取消方法
	// fairSyncCancel context.CancelFunc
}

// New returns a new replica.
func New(conf Config, builder modules.Builder) (replica *Replica) {
	clientSrvOpts := conf.ClientServerOptions

	if conf.TLS {
		clientSrvOpts = append(clientSrvOpts, gorums.WithGRPCServerOptions(
			grpc.Creds(credentials.NewServerTLSFromCert(conf.Certificate)),
		))
	}
	// 初始化clientSrv
	clientSrv := newClientServer(conf, clientSrvOpts)

	srv := &Replica{
		clientSrv:    clientSrv,
		execHandlers: make(map[cmdID]func(*emptypb.Empty, error)), // 这个参数就初始化用了？
		cancel:       func() {},
		// fairSyncCancel: func() {}, // RapidFair
		done: make(chan struct{}),
	}

	replicaSrvOpts := conf.ReplicaServerOptions
	if conf.TLS {
		replicaSrvOpts = append(replicaSrvOpts, gorums.WithGRPCServerOptions(
			grpc.Creds(credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{*conf.Certificate},
				ClientCAs:    conf.RootCAs,
				ClientAuth:   tls.RequireAndVerifyClientCert,
			})),
		))
	}
	// 初始化backend server
	srv.hsSrv = backend.NewServer(replicaSrvOpts...)

	var creds credentials.TransportCredentials
	managerOpts := conf.ManagerOptions
	if conf.TLS {
		creds = credentials.NewTLS(&tls.Config{
			RootCAs:      conf.RootCAs,
			Certificates: []tls.Certificate{*conf.Certificate},
		})
	}
	// 初始化backend config
	srv.cfg = backend.NewConfig(creds, managerOpts...)

	builder.Add(
		srv.cfg,   // configuration
		srv.hsSrv, // event handling

		modules.ExtendedExecutor(srv.clientSrv),
		modules.ExtendedForkHandler(srv.clientSrv),
		srv.clientSrv.cmdCache,
		srv.clientSrv.cmdCache,
	)
	srv.hs = builder.Build() // Build()方法初始化所有加入列表的模块

	return srv
}

// Modules returns the Modules object of this replica.
func (srv *Replica) Modules() *modules.Core {
	return srv.hs
}

// StartServers starts the client and replica servers.
func (srv *Replica) StartServers(replicaListen, clientListen net.Listener) {
	srv.hsSrv.StartOnListener(replicaListen)
	srv.clientSrv.StartOnListener(clientListen)
}

// Connect connects to the other replicas.
func (srv *Replica) Connect(replicas []backend.ReplicaInfo) error {
	return srv.cfg.Connect(replicas)
}

// Start runs the replica in a goroutine.
func (srv *Replica) Start() {
	var ctx context.Context
	ctx, srv.cancel = context.WithCancel(context.Background())

	// RapidFair:为启动fairSynchronizer创建新的上下文
	// var fairSyncCtx context.Context
	// fairSyncCtx, srv.fairSyncCancel = context.WithCancel(context.Background())

	go func() {
		srv.RunFairSync(ctx)
	}()

	go func() {
		srv.Run(ctx)    // Run方法会被eventloop持续阻塞
		close(srv.done) // 结束Run()之后关闭通道
	}()
}

// Stop stops the replica and closes connections.
func (srv *Replica) Stop() {
	srv.cancel() // 调用cancel()取消上下文
	// srv.fairSyncCancel() // RapidFair: 停止fairSynchronizer
	<-srv.done // 如果channel中没有值，则会阻塞后续语句；channel关闭后，读取channel不会阻塞；因此在确定调用了close(srv.done)之后，这里才能继续执行后续的srv.Close()方法
	srv.Close()
}

// Run runs the replica until the context is cancelled.
// 这里的ctx用于控制synchronizer和eventLoop的整体生命周期？
func (srv *Replica) Run(ctx context.Context) {
	var (
		synchronizer modules.Synchronizer
		eventLoop    *eventloop.EventLoop
	)
	srv.hs.Get(&synchronizer, &eventLoop)
	synchronizer.Start(ctx)
	// eventloop的方法会持续循环执行，阻塞；使得Start中的协程可以一直运行？
	fmt.Println("===========================CAN RUN EVENTLOOP===========================")
	eventLoop.Run(ctx)
}

// RapidFair:
func (srv *Replica) RunFairSync(ctx context.Context) {
	var opts *modules.Options
	srv.hs.Get(&opts)
	// RapidFair: 启动fairSynchronizer【两个同步器是不是不应该使用同一个上下文？】
	if opts.UseRapidFair() {
		fmt.Println("===========================CAN RUN fairSynchronizer===========================")
		var fairSynchronizer modules.FairSynchronizer
		srv.hs.Get(&fairSynchronizer)
		fairSynchronizer.Start(ctx)
	}
}

// Close closes the connections and stops the servers used by the replica.
func (srv *Replica) Close() {
	srv.clientSrv.Stop()
	srv.cfg.Close()
	srv.hsSrv.Stop()
}

// GetHash returns the hash of all executed commands.
func (srv *Replica) GetHash() (b []byte) {
	return srv.clientSrv.hash.Sum(b)
}
