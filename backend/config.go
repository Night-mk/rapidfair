// Package backend implements the networking backend for hotstuff using the Gorums framework.
package backend

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"

	"github.com/relab/gorums"
	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/internal/proto/hotstuffpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// Replica provides methods used by hotstuff to send messages to replicas.
type Replica struct {
	node               *hotstuffpb.Node
	id                 hotstuff.ID
	pubKey             hotstuff.PublicKey
	voteCancel         context.CancelFunc
	newViewCancel      context.CancelFunc
	collectCancel      context.CancelFunc // RapidFair: 为Collect方法提供上下文取消
	multiCollectCancel context.CancelFunc // RapidFair: 为MultiCollect方法提供上下文取消
	md                 map[string]string
}

// ID returns the replica's ID.
func (r *Replica) ID() hotstuff.ID {
	return r.id
}

// PublicKey returns the replica's public key.
func (r *Replica) PublicKey() hotstuff.PublicKey {
	return r.pubKey
}

// Vote sends the partial certificate to the other replica. 发送hotstuff定义的证书给其他阶节点
func (r *Replica) Vote(cert hotstuff.PartialCert) {
	if r.node == nil {
		return
	}
	var ctx context.Context
	r.voteCancel()
	ctx, r.voteCancel = context.WithCancel(context.Background())
	pCert := hotstuffpb.PartialCertToProto(cert)
	// 调用hotstuff_gorums.pb.go的Vote()方法发给所有其他人
	r.node.Vote(ctx, pCert, gorums.WithNoSendWaiting())
}

// RapidFair: baseline collect发送交易序列（或交易序列哈希）给其他replica
func (r *Replica) Collect(col hotstuff.CollectTxSeq) {
	if r.node == nil {
		return
	}
	var ctx context.Context
	r.collectCancel()
	// 初始化collect的上下文
	ctx, r.collectCancel = context.WithCancel(context.Background())
	cTxSeq := hotstuffpb.CollectToProto(col)
	// 调用fairorder_gorums.pb.go的Collect()发送collect给其他节点，怎么确定发送给leader？
	r.node.Collect(ctx, cTxSeq, gorums.WithNoSendWaiting())
}

// RapidFair: MultiCollect，replica发送消息给Leader
func (r *Replica) MultiCollect(mc hotstuff.MultiCollectMsg) {
	if r.node == nil {
		return
	}
	var ctx context.Context
	r.multiCollectCancel()
	// 初始化collect的上下文
	ctx, r.multiCollectCancel = context.WithCancel(context.Background())
	// 调用fairorder_gorums.pb.go的Collect()发送collect给其他节点，怎么确定发送给leader？
	r.node.MultiCollect(ctx, hotstuffpb.MultiCollectToProto(mc), gorums.WithNoSendWaiting())
}

// NewView sends the quorum certificate to the other replica.
// 发送qc给其他节点
func (r *Replica) NewView(msg hotstuff.SyncInfo) {
	if r.node == nil {
		return
	}
	var ctx context.Context
	r.newViewCancel()
	ctx, r.newViewCancel = context.WithCancel(context.Background())
	r.node.NewView(ctx, hotstuffpb.SyncInfoToProto(msg), gorums.WithNoSendWaiting())
}

// Metadata returns the gRPC metadata from this replica's connection.
func (r *Replica) Metadata() map[string]string {
	return r.md
}

// Config holds information about the current configuration of replicas that participate in the protocol,
// and some information about the local replica. It also provides methods to send messages to the other replicas.
type Config struct {
	opts      []gorums.ManagerOption
	connected bool

	mgr *hotstuffpb.Manager
	subConfig
}

// subConfig实现了modules.Configuration接口
type subConfig struct {
	eventLoop    *eventloop.EventLoop
	logger       logging.Logger
	opts         *modules.Options
	synchronizer modules.Synchronizer

	cfg      *hotstuffpb.Configuration
	replicas map[hotstuff.ID]modules.Replica
}

// InitModule initializes the configuration.
func (cfg *Config) InitModule(mods *modules.Core) {
	mods.Get(
		&cfg.eventLoop,
		&cfg.logger,
		&cfg.subConfig.opts,
		&cfg.synchronizer,
	)

	// We delay processing `replicaConnected` events until after the configurations `connected` event has occurred.
	cfg.eventLoop.RegisterHandler(replicaConnected{}, func(event any) {
		if !cfg.connected {
			cfg.eventLoop.DelayUntil(ConnectedEvent{}, event)
			return
		}
		cfg.replicaConnected(event.(replicaConnected))
	})
}

// NewConfig creates a new configuration.
func NewConfig(creds credentials.TransportCredentials, opts ...gorums.ManagerOption) *Config {
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
		grpc.WithTransportCredentials(creds),
	}
	opts = append(opts, gorums.WithGrpcDialOptions(grpcOpts...))

	// initialization will be finished by InitModule
	cfg := &Config{
		subConfig: subConfig{
			replicas: make(map[hotstuff.ID]modules.Replica),
		},
		opts: opts,
	}
	return cfg
}

func (cfg *Config) replicaConnected(c replicaConnected) {
	info, peerok := peer.FromContext(c.ctx)
	md, mdok := metadata.FromIncomingContext(c.ctx)
	if !peerok || !mdok {
		return
	}

	id, err := GetPeerIDFromContext(c.ctx, cfg)
	if err != nil {
		cfg.logger.Warnf("Failed to get id for %v: %v", info.Addr, err)
		return
	}

	replica, ok := cfg.replicas[id]
	if !ok {
		cfg.logger.Warnf("Replica with id %d was not found", id)
		return
	}

	replica.(*Replica).md = readMetadata(md)

	cfg.logger.Debugf("Replica %d connected from address %v", id, info.Addr)
}

const keyPrefix = "hotstuff-"

func mapToMetadata(m map[string]string) metadata.MD {
	md := metadata.New(nil)
	for k, v := range m {
		md.Set(keyPrefix+k, v)
	}
	return md
}

func readMetadata(md metadata.MD) map[string]string {
	m := make(map[string]string)
	for k, values := range md {
		if _, key, ok := strings.Cut(k, keyPrefix); ok {
			m[key] = values[0]
		}
	}
	return m
}

// GetRawConfiguration returns the underlying gorums RawConfiguration.
func (cfg *Config) GetRawConfiguration() gorums.RawConfiguration {
	return cfg.cfg.RawConfiguration
}

// ReplicaInfo holds information about a replica.
type ReplicaInfo struct {
	ID      hotstuff.ID
	Address string
	PubKey  hotstuff.PublicKey
}

// Connect opens connections to the replicas in the configuration.
// RapidFair: baseline 增加参数初始化
func (cfg *Config) Connect(replicas []ReplicaInfo) (err error) {
	opts := cfg.opts
	cfg.opts = nil // options are not needed beyond this point, so we delete them.

	md := mapToMetadata(cfg.subConfig.opts.ConnectionMetadata())

	// embed own ID to allow other replicas to identify messages from this replica
	md.Set("id", fmt.Sprintf("%d", cfg.subConfig.opts.ID()))

	opts = append(opts, gorums.WithMetadata(md))

	cfg.mgr = hotstuffpb.NewManager(opts...)

	// set up an ID mapping to give to gorums
	idMapping := make(map[string]uint32, len(replicas))
	for _, replica := range replicas {
		// also initialize Replica structures
		cfg.replicas[replica.ID] = &Replica{
			id:                 replica.ID,
			pubKey:             replica.PubKey,
			newViewCancel:      func() {},
			voteCancel:         func() {},
			collectCancel:      func() {}, // RapidFair: baseline新增collectCancel初始化
			multiCollectCancel: func() {},
			md:                 make(map[string]string),
		}
		// we do not want to connect to ourself
		if replica.ID != cfg.subConfig.opts.ID() {
			idMapping[replica.Address] = uint32(replica.ID)
		}
	}

	// this will connect to the replicas
	cfg.cfg, err = cfg.mgr.NewConfiguration(qspec{}, gorums.WithNodeMap(idMapping))
	if err != nil {
		return fmt.Errorf("failed to create configuration: %w", err)
	}

	// now we need to update the "node" field of each replica we connected to
	for _, node := range cfg.cfg.Nodes() {
		// the node ID should correspond with the replica ID
		// because we already configured an ID mapping for gorums to use.
		id := hotstuff.ID(node.ID())
		replica := cfg.replicas[id].(*Replica)
		replica.node = node
		// RapidFair: baseline初始化fairnode
		// replica.fairnode = node
	}

	cfg.connected = true

	// this event is sent so that any delayed `replicaConnected` events can be processed.
	cfg.eventLoop.AddEvent(ConnectedEvent{})

	return nil
}

// Replicas returns all of the replicas in the configuration.
func (cfg *subConfig) Replicas() map[hotstuff.ID]modules.Replica {
	return cfg.replicas
}

// Replica returns a replica if it is present in the configuration.
func (cfg *subConfig) Replica(id hotstuff.ID) (replica modules.Replica, ok bool) {
	replica, ok = cfg.replicas[id]
	return
}

// SubConfig returns a subconfiguration containing the replicas specified in the ids slice.
func (cfg *Config) SubConfig(ids []hotstuff.ID) (sub modules.Configuration, err error) {
	replicas := make(map[hotstuff.ID]modules.Replica)
	nids := make([]uint32, len(ids))
	for i, id := range ids {
		nids[i] = uint32(id)
		replicas[id] = cfg.replicas[id]
	}
	newCfg, err := cfg.mgr.NewConfiguration(gorums.WithNodeIDs(nids))
	if err != nil {
		return nil, err
	}
	return &subConfig{
		eventLoop:    cfg.eventLoop,
		logger:       cfg.logger,
		opts:         cfg.subConfig.opts,
		synchronizer: cfg.synchronizer,
		cfg:          newCfg,
		replicas:     replicas,
	}, nil
}

func (cfg *subConfig) SubConfig(_ []hotstuff.ID) (_ modules.Configuration, err error) {
	return nil, errors.New("not supported")
}

// Len returns the number of replicas in the configuration.
func (cfg *subConfig) Len() int {
	return len(cfg.replicas)
}

// QuorumSize returns the size of a quorum
func (cfg *subConfig) QuorumSize() int {
	return hotstuff.QuorumSize(cfg.Len())
}

// RapidFair: 返回公平排序定义下的quorumsize
func (cfg *subConfig) QuorumSizeFair() int {
	return hotstuff.QuorumSizeFair(cfg.Len(), cfg.opts.ThemisGamma())
}

// Propose sends the block to all replicas in the configuration
// config是发送block给所有节点
func (cfg *subConfig) Propose(proposal hotstuff.ProposeMsg) {
	if cfg.cfg == nil {
		return
	}
	cfg.cfg.Propose(
		cfg.synchronizer.ViewContext(),
		hotstuffpb.ProposalToProto(proposal),
		gorums.WithNoSendWaiting(),
	)
}

// Timeout sends the timeout message to all replicas.
func (cfg *subConfig) Timeout(msg hotstuff.TimeoutMsg) {
	if cfg.cfg == nil {
		return
	}
	cfg.cfg.Timeout(
		cfg.synchronizer.ViewContext(),
		hotstuffpb.TimeoutMsgToProto(msg),
		gorums.WithNoSendWaiting(),
	)
}

// Fetch requests a block from all the replicas in the configuration
func (cfg *subConfig) Fetch(ctx context.Context, hash hotstuff.Hash) (*hotstuff.Block, bool) {
	protoBlock, err := cfg.cfg.Fetch(ctx, &hotstuffpb.BlockHash{Hash: hash[:]})
	if err != nil {
		qcErr, ok := err.(gorums.QuorumCallError)
		// filter out context errors
		if !ok || (qcErr.Reason != context.Canceled.Error() && qcErr.Reason != context.DeadlineExceeded.Error()) {
			cfg.logger.Infof("Failed to fetch block: %v", err)
		}
		return nil, false
	}
	return hotstuffpb.BlockFromProto(protoBlock), true
}

// RapidFair: baseline leader广播ReadyCollectMsg给所有replica
func (cfg *subConfig) ReadyCollect(rc hotstuff.ReadyCollectMsg) {
	if cfg.cfg == nil {
		return
	}
	cfg.cfg.ReadyCollect(
		cfg.synchronizer.ViewContext(),
		hotstuffpb.ReadyCollectMsgToProto(rc),
		gorums.WithNoSendWaiting(),
	)
}

// RapidFair: 实现发送端广播collect消息给所有replica
// func (cfg *subConfig) MultiCollect(mc hotstuff.MultiCollectMsg) {
// 	if cfg.cfg == nil {
// 		return
// 	}
// 	cfg.cfg.MultiCollect(
// 		cfg.synchronizer.ViewContext(),
// 		hotstuffpb.MultiCollectToProto(mc),
// 		gorums.WithNoSendWaiting(),
// 	)
// }

// RapidFair: 实现发送端leader广播PreNotify消息给所有replica
func (cfg *subConfig) PreNotify(pn hotstuff.PreNotifyMsg) {
	if cfg.cfg == nil {
		return
	}
	cfg.cfg.PreNotify(
		cfg.synchronizer.ViewContext(),
		hotstuffpb.PreNotifyToProto(pn),
		gorums.WithNoSendWaiting(),
	)
}

// Close closes all connections made by this configuration.
func (cfg *Config) Close() {
	cfg.mgr.Close()
}

var _ modules.Configuration = (*Config)(nil)

type qspec struct{}

// FetchQF is the quorum function for the Fetch quorum call method.
// It simply returns true if one of the replies matches the requested block.
func (q qspec) FetchQF(in *hotstuffpb.BlockHash, replies map[uint32]*hotstuffpb.Block) (*hotstuffpb.Block, bool) {
	var h hotstuff.Hash
	copy(h[:], in.GetHash())
	for _, b := range replies {
		block := hotstuffpb.BlockFromProto(b)
		if h == block.Hash() {
			return b, true
		}
	}
	return nil, false
}

// ConnectedEvent is sent when the configuration has connected to the other replicas.
type ConnectedEvent struct{}
