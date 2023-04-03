package tsfchain

// 实现modules.TSFChain接口, 维护一组TxSeqFragment

import (
	"sync"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
)

type txseqfragmentChain struct {
	synchronizer     modules.Synchronizer
	fairSynchronizer modules.FairSynchronizer
	logger           logging.Logger
	col              modules.Collector

	mut              sync.Mutex
	fragments        map[hotstuff.Hash]*hotstuff.TxSeqFragment // 按哈希值索引fragment
	fragmentAtHeight map[hotstuff.View]*hotstuff.TxSeqFragment // 按virtual view索引fragment
}

func (tsfchain *txseqfragmentChain) InitModule(mods *modules.Core) {
	mods.Get(
		&tsfchain.synchronizer,
		&tsfchain.fairSynchronizer,
		&tsfchain.logger,
		&tsfchain.col,
	)
}

func New() modules.TSFChain {
	tsfc := &txseqfragmentChain{
		fragments:        make(map[hotstuff.Hash]*hotstuff.TxSeqFragment),
		fragmentAtHeight: make(map[hotstuff.View]*hotstuff.TxSeqFragment),
	}
	// 使用genesis初始化第一个tsf
	tsfc.Store(hotstuff.GetGenesisTSF())
	return tsfc
}

// 缓存提出的fragment到本地缓存
func (tsfchain *txseqfragmentChain) Store(fragment *hotstuff.TxSeqFragment) {
	tsfchain.mut.Lock()
	defer tsfchain.mut.Unlock()
	// 在fragmentChain中缓存已经提出的fragment
	tsfchain.fragments[fragment.Hash()] = fragment
	tsfchain.fragmentAtHeight[fragment.VirView()] = fragment
	// 暂时不考虑从远程fetch的过程
}

// 从本地按hash值获取fragment
func (tsfchain *txseqfragmentChain) LocalGet(hash hotstuff.Hash) (*hotstuff.TxSeqFragment, bool) {
	tsfchain.mut.Lock()
	defer tsfchain.mut.Unlock()

	fragment, ok := tsfchain.fragments[hash]
	if !ok {
		return nil, false
	}

	return fragment, true
}

// 从本地按view获取fragment
func (tsfchain *txseqfragmentChain) LocalGetAtHeight(v hotstuff.View) (*hotstuff.TxSeqFragment, bool) {
	tsfchain.mut.Lock()
	defer tsfchain.mut.Unlock()

	fragment, ok := tsfchain.fragmentAtHeight[v]
	if !ok {
		return nil, false
	}

	return fragment, true
}

// 构建Get方法从本地、远程按hash获取对应的fragment
func (tsfchain *txseqfragmentChain) Get(hash hotstuff.Hash) (fragment *hotstuff.TxSeqFragment, ok bool) {
	// 暂时先不考虑从远程fetch的问题？
	tsfchain.mut.Lock()
	fragment, ok = tsfchain.fragments[hash]
	if ok {
		goto done
	}

	// 研究增加使用fetch从其他节点获取fragment的方法

done:
	defer tsfchain.mut.Unlock()

	if !ok {
		return nil, false
	}

	return fragment, true
}
