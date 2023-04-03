package hotstuff

var genesisBlock = NewBlock(Hash{}, QuorumCert{}, "", 0, 0)

// GetGenesis returns a pointer to the genesis block, the starting point for the hotstuff blockchain.
func GetGenesis() *Block {
	return genesisBlock
}

// RapidFair

// RapidFair: 构建初始fragment
var genesisFragment = NewFragment(0, 0, "", make(TXList), "", make(TXList))

func GetGenesisFragment() *Fragment {
	return genesisFragment
}

// 构建初始TxSeqFragment
var genesisTxSeqFragment = NewTxSeqFragment(Hash{}, QuorumCert{}, 0, 0, make(TXList))

func GetGenesisTSF() *TxSeqFragment {
	return genesisTxSeqFragment
}
