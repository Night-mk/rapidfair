package backend

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/relab/hotstuff/eventloop"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"

	"github.com/relab/gorums"
	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/internal/proto/hotstuffpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Server is the Server-side of the gorums backend.
// It is responsible for calling handler methods on the consensus instance.
type Server struct {
	blockChain    modules.BlockChain
	configuration modules.Configuration
	eventLoop     *eventloop.EventLoop
	eventLoopFair *eventloop.EventLoopFair // RapidFair: 为OFO增加单独的事件调用流
	logger        logging.Logger

	gorumsSrv *gorums.Server
}

// InitModule initializes the Server.
func (srv *Server) InitModule(mods *modules.Core) {
	mods.Get(
		&srv.eventLoop,
		&srv.eventLoopFair,
		&srv.configuration,
		&srv.blockChain,
		&srv.logger,
	)
}

// NewServer creates a new Server.
func NewServer(opts ...gorums.ServerOption) *Server {
	srv := &Server{}

	opts = append(opts, gorums.WithConnectCallback(func(ctx context.Context) {
		srv.eventLoop.AddEvent(replicaConnected{ctx})
	}))

	srv.gorumsSrv = gorums.NewServer(opts...)

	hotstuffpb.RegisterHotstuffServer(srv.gorumsSrv, &serviceImpl{srv})
	return srv
}

// GetGorumsServer returns the underlying gorums Server.
func (srv *Server) GetGorumsServer() *gorums.Server {
	return srv.gorumsSrv
}

// Start creates a listener on the configured address and starts the server.
func (srv *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	srv.StartOnListener(lis)
	return nil
}

// StartOnListener starts the server with the given listener.
func (srv *Server) StartOnListener(listener net.Listener) {
	go func() {
		err := srv.gorumsSrv.Serve(listener)
		if err != nil {
			srv.logger.Errorf("An error occurred while serving: %v", err)
		}
	}()
}

// GetPeerIDFromContext extracts the ID of the peer from the context.
func GetPeerIDFromContext(ctx context.Context, cfg modules.Configuration) (hotstuff.ID, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return 0, fmt.Errorf("peerInfo not available")
	}

	if peerInfo.AuthInfo != nil && peerInfo.AuthInfo.AuthType() == "tls" {
		tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
		if !ok {
			return 0, fmt.Errorf("authInfo of wrong type: %T", peerInfo.AuthInfo)
		}
		if len(tlsInfo.State.PeerCertificates) > 0 {
			cert := tlsInfo.State.PeerCertificates[0]
			for replicaID := range cfg.Replicas() {
				if subject, err := strconv.Atoi(cert.Subject.CommonName); err == nil && hotstuff.ID(subject) == replicaID {
					return replicaID, nil
				}
			}
		}
		return 0, fmt.Errorf("could not find matching certificate")
	}

	// If we're not using TLS, we'll fallback to checking the metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, fmt.Errorf("metadata not available")
	}

	v := md.Get("id")
	if len(v) < 1 {
		return 0, fmt.Errorf("id field not present")
	}

	id, err := strconv.Atoi(v[0])
	if err != nil {
		return 0, fmt.Errorf("cannot parse ID field: %w", err)
	}

	return hotstuff.ID(id), nil
}

// Stop stops the server.
func (srv *Server) Stop() {
	srv.gorumsSrv.Stop()
}

// serviceImpl provides the implementation of the HotStuff gorums service.
type serviceImpl struct {
	srv *Server
}

// Propose handles a replica's response to the Propose QC from the leader.
func (impl *serviceImpl) Propose(ctx gorums.ServerCtx, proposal *hotstuffpb.Proposal) {
	id, err := GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Failed to get client ID: %v", err)
		return
	}

	proposal.Block.Proposer = uint32(id)
	proposeMsg := hotstuffpb.ProposalFromProto(proposal)
	proposeMsg.ID = id

	impl.srv.eventLoop.AddEvent(proposeMsg)
}

// Vote handles an incoming vote message.
func (impl *serviceImpl) Vote(ctx gorums.ServerCtx, cert *hotstuffpb.PartialCert) {
	id, err := GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Failed to get client ID: %v", err)
		return
	}

	impl.srv.eventLoop.AddEvent(hotstuff.VoteMsg{
		ID:          id,
		PartialCert: hotstuffpb.PartialCertFromProto(cert),
	})
}

// NewView handles the leader's response to receiving a NewView rpc from a replica.
// leader从replica接收 NewView rpc 的响应处理，这样leader本地会继续执行OnNewView()方法吗？
func (impl *serviceImpl) NewView(ctx gorums.ServerCtx, msg *hotstuffpb.SyncInfo) {
	id, err := GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Failed to get client ID: %v", err)
		return
	}

	impl.srv.eventLoop.AddEvent(hotstuff.NewViewMsg{
		ID:       id,
		SyncInfo: hotstuffpb.SyncInfoFromProto(msg),
	})
}

// Fetch handles an incoming fetch request.
func (impl *serviceImpl) Fetch(ctx gorums.ServerCtx, pb *hotstuffpb.BlockHash) (*hotstuffpb.Block, error) {
	var hash hotstuff.Hash
	copy(hash[:], pb.GetHash())

	block, ok := impl.srv.blockChain.LocalGet(hash)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "requested block was not found")
	}

	impl.srv.logger.Debugf("OnFetch: %.8s", hash)

	return hotstuffpb.BlockToProto(block), nil
}

// Timeout handles an incoming TimeoutMsg.
func (impl *serviceImpl) Timeout(ctx gorums.ServerCtx, msg *hotstuffpb.TimeoutMsg) {
	var err error
	timeoutMsg := hotstuffpb.TimeoutMsgFromProto(msg)
	timeoutMsg.ID, err = GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Could not get ID of replica: %v", err)
	}
	impl.srv.eventLoop.AddEvent(timeoutMsg)
}

type replicaConnected struct {
	ctx context.Context
}

// RapidFair: Collect处理远程replica节点输入的proto collect消息
func (impl *serviceImpl) Collect(ctx gorums.ServerCtx, col *hotstuffpb.CollectTxSeq) {
	// 从context中获取发送消息的replica的ID
	id, err := GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Failed to get client ID: %v", err)
		return
	}

	// 将构建的collectMsg传输给eventLoop处理， AddEvent主要是增加事件
	impl.srv.eventLoop.AddEvent(hotstuff.CollectMsg{
		ID:           id,
		CollectTxSeq: hotstuffpb.CollectFromProto(col),
	})
}

// ReadyCollect处理leader节点输入的proto <ReadyCollectMsg>消息
func (impl *serviceImpl) ReadyCollect(ctx gorums.ServerCtx, rc *hotstuffpb.ReadyCollectMsg) {
	// 从context中获取发送消息的replica的ID
	var err error
	rcMsg := hotstuffpb.ReadyCollectMsgFromProto(rc)
	rcMsg.ID, err = GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Failed to get client ID: %v", err)
		return
	}

	// AddEvent增加事件ReadyCollectMsg传输给eventLoop处理
	impl.srv.eventLoop.AddEvent(rcMsg)
}

// RapidFair: 处理广播接收到的Collect消息（这里的view是virtual view）

func (impl *serviceImpl) MultiCollect(ctx gorums.ServerCtx, mc *hotstuffpb.MCollect) {
	// 从context中获取发送消息的replica的ID
	var err error
	mcMsg := hotstuffpb.MultiCollectFromProto(mc)
	mcMsg.ID, err = GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Failed to get client ID: %v", err)
		return
	}
	// AddEvent增加事件MultiCollectMsg传输给eventLoopFair处理
	// impl.srv.eventLoop.AddEvent(mcMsg)
	impl.srv.eventLoopFair.AddEvent(mcMsg)
}

// RapidFair: 处理接收到的 proto.PreNotifyMsg
func (impl *serviceImpl) PreNotify(ctx gorums.ServerCtx, pn *hotstuffpb.PreNotifyMsg) {
	// 从context中获取发送消息的replica的ID
	var err error
	pnMsg := hotstuffpb.PreNotifyFromProto(pn)
	pnMsg.ID, err = GetPeerIDFromContext(ctx, impl.srv.configuration)
	if err != nil {
		impl.srv.logger.Infof("Failed to get client ID: %v", err)
		return
	}
	// AddEvent增加事件PreNotifyMsg传输给eventLoopFair处理
	impl.srv.eventLoopFair.AddEvent(pnMsg)
}
