package fragmentchain

// 实现modules.FragmentChain接口

import (
	"sync"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/logging"
	"github.com/relab/hotstuff/modules"
)

type fragmentChain struct {
	synchronizer     modules.Synchronizer
	fairSynchronizer modules.FairSynchronizer
	logger           logging.Logger
	col              modules.Collector
	// commandQueue     modules.CommandQueue

	mut              sync.Mutex
	fragments        map[hotstuff.Hash]*hotstuff.Fragment // 按哈希值索引fragment
	fragmentAtHeight map[hotstuff.View]*hotstuff.Fragment // 按virtual view索引fragment
}

func (fchain *fragmentChain) InitModule(mods *modules.Core) {
	mods.Get(
		&fchain.synchronizer,
		&fchain.fairSynchronizer,
		&fchain.logger,
		&fchain.col,
		// &fchain.commandQueue,
	)
}

func New() modules.FragmentChain {
	fc := &fragmentChain{
		fragments:        make(map[hotstuff.Hash]*hotstuff.Fragment),
		fragmentAtHeight: make(map[hotstuff.View]*hotstuff.Fragment),
	}
	// 使用genesis初始化第一个fragment
	gf := hotstuff.GetGenesisFragment()
	fc.Store(gf)
	// fc.commandQueue.AddFragment(gf)
	return fc
}

// 缓存提出的fragment到本地缓存
func (fchain *fragmentChain) Store(fragment *hotstuff.Fragment) {
	fchain.mut.Lock()
	defer fchain.mut.Unlock()
	// 在fragmentChain中缓存已经提出的fragment
	fchain.fragments[fragment.Hash()] = fragment
	fchain.fragmentAtHeight[fragment.VirView()] = fragment
}

// 从本地按hash值获取fragment
func (fchain *fragmentChain) LocalGet(hash hotstuff.Hash) (*hotstuff.Fragment, bool) {
	fchain.mut.Lock()
	defer fchain.mut.Unlock()

	fragment, ok := fchain.fragments[hash]
	if !ok {
		return nil, false
	}

	return fragment, true
}

// 从本地按view获取fragment
func (fchain *fragmentChain) LocalGetAtHeight(v hotstuff.View) (*hotstuff.Fragment, bool) {
	fchain.mut.Lock()
	defer fchain.mut.Unlock()

	fragment, ok := fchain.fragmentAtHeight[v]
	if !ok {
		return nil, false
	}

	return fragment, true
}

// 构建Get方法从本地、远程按hash获取对应的fragment
func (fchain *fragmentChain) Get(hash hotstuff.Hash) (fragment *hotstuff.Fragment, ok bool) {
	// 暂时先不考虑从远程fetch的问题？
	fchain.mut.Lock()
	fragment, ok = fchain.fragments[hash]
	if ok {
		goto done
	}

	// 研究增加使用fetch从其他节点获取fragment的方法

done:
	defer fchain.mut.Unlock()

	if !ok {
		return nil, false
	}

	return fragment, true
}
