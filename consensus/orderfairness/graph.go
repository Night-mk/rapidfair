package orderfairness

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
)

// building blocks
// ==============构建图和顶点类、实例==============
// =========顶点类=========
type VT string // vertex type
type Vertex struct {
	value VT
}

func (v *Vertex) Print() string {
	return fmt.Sprintf("%v", v.value)
}

// =========图类=========
// 顶点：[tx1, tx2, ...] -> vertex
// 边：[tx1][tx2] -> vertex
type Graph struct {
	sync.RWMutex // 读写锁
	vertList     map[VT]*Vertex
	edgeList     map[VT]map[VT]*Vertex
}

// 初始化图
func NewGraph() *Graph {
	g := &Graph{
		vertList: make(map[VT]*Vertex),
		edgeList: make(map[VT]map[VT]*Vertex),
	}

	return g
}

// 增加顶点
func (g *Graph) addVertex(key VT) {
	g.Lock()
	defer g.Unlock()
	if _, ok := g.vertList[key]; !ok { // 如果顶点不存在
		v := &Vertex{value: key}
		g.vertList[key] = v
		// 如果边的列表里没有，则要初始化
		if g.edgeList[key] == nil {
			g.edgeList[key] = make(map[VT]*Vertex)
		}
	}
}

// 获取顶点
func (g *Graph) getVertex(key VT) (bool, *Vertex) {
	v, ok := g.vertList[key]
	return ok, v
}

// 删除顶点
func (g *Graph) deleteVertex(key VT) {
	g.Lock()
	defer g.Unlock()
	if g.vertList[key] != nil {
		delete(g.vertList, key)
	}
	// 如果顶点在edgelist中，则删除edgelist中的内容
	if g.edgeList[key] != nil {
		delete(g.edgeList, key)
	}
}

// 增加边（有向边）
func (g *Graph) addEdge(v1 VT, v2 VT) {
	g.Lock()
	defer g.Unlock()
	if g.edgeList == nil {
		g.edgeList = make(map[VT]map[VT]*Vertex)
	}
	if _, ok := g.vertList[v1]; !ok {
		g.vertList[v1] = &Vertex{value: v1}
	}
	if _, ok1 := g.vertList[v2]; !ok1 {
		g.vertList[v2] = &Vertex{value: v2}
	}

	if g.edgeList[v1] == nil {
		g.edgeList[v1] = make(map[VT]*Vertex)
	}
	g.edgeList[v1][v2] = &Vertex{value: v2}
}

// 删除边
func (g *Graph) deleteEdge(v1 VT, v2 VT) {
	g.Lock()
	defer g.Unlock()
	if _, ok := g.edgeList[v1][v2]; ok {
		delete(g.edgeList[v1], v2)
	}
}

// 获取图的所有顶点key
func (g *Graph) getVertices() []VT {
	g.Lock()
	defer g.Unlock()
	verts := make([]VT, 0, len(g.vertList))
	for k := range g.vertList {
		verts = append(verts, k)
	}
	return verts
}

// 获取图中顶点v的边列表
func (g *Graph) getEdges(v VT) []VT {
	g.Lock()
	defer g.Unlock()
	edges := make([]VT, 0, len(g.edgeList[v]))
	if len(g.edgeList[v]) > 0 {
		for k := range g.edgeList[v] {
			edges = append(edges, k)
		}
	}
	return edges
}

// 打印有向图
func (g *Graph) Print() {
	g.Lock()
	defer g.Unlock()
	ret := ""
	for k, v := range g.vertList {
		ret += v.Print() + "->"
		neighbors := g.edgeList[k]
		for _, v1 := range neighbors {
			ret += v1.Print() + " "
		}
		ret += "\n"
	}
	fmt.Println(ret)
}

// 公平排序算法部分
// =========利用交易序列构建有向图并排序=========
// 1. 从n-f个交易序列中获取顶点，查找数量超过n-2f的顶点加入有向图
func findVertex(txList [][]string, n int, f int) ([]string, [][]string) {
	vertNum := make(map[string]int)
	var vertList []string
	threshold := n - 2*f

	failVerts := make(map[string]int)
	txListNew := make([][]string, len(txList))
	// for i, _ := range txListCopy { // 深度copy当前的txList
	// 	txListCopy[i] = make([]string, len(txList[i]))
	// 	copy(txListCopy[i], txList[i])
	// }

	// 计算交易出现的数量
	for _, v1 := range txList {
		for _, v2 := range v1 {
			if _, ok := vertNum[v2]; !ok {
				vertNum[v2] = 1
			} else {
				vertNum[v2] += 1
			}
		}
	}
	// 计算能加入有向图的交易顶点
	for k, v := range vertNum {
		if v >= threshold {
			vertList = append(vertList, k)
		} else {
			failVerts[k] = 0
		}
	}
	// 构建新的交易序列
	// 删除交易序列中不能加入图的交易
	if len(failVerts) > 0 {
		for k1, v1 := range txList {
			txListNew[k1] = make([]string, 0, 0)
			for _, v2 := range v1 {
				if _, ok := failVerts[v2]; !ok { // 把不在fail中的交易加入新txList
					txListNew[k1] = append(txListNew[k1], v2)
				}
			}
		}
	}

	return vertList, txListNew
}

// 计算交易有向图的边列表
// 输入的txList只包含足够数量的交易
func findEdges(txList [][]string, vertList []string, n int, f int, gamma float32) (txEdge []string) {
	//构建M*M大小的二维字典
	txDict := make(map[string]map[string]int)
	for _, i := range vertList {
		txDict[i] = make(map[string]int)
		for _, j := range vertList {
			txDict[i][j] = 0
		}
	}
	// 计算所有交易序列中，交易对(m,m')的数量
	for _, x := range txList {
		for k1, v1 := range x {
			if k1 < len(x)-1 {
				for k2 := k1 + 1; k2 < len(x); k2++ {
					txDict[v1][x[k2]] += 1
				}
			}
		}
	}
	// 筛选edge M[m][m']加入有向图
	threshold := float32(n)*(float32(1)-gamma) + float32(f) + 1
	// 输出数组["tx1,tx2","tx1,tx3",...]
	for k1, v1 := range vertList {
		if k1 < len(vertList)-1 {
			for k2 := k1 + 1; k2 < len(vertList); k2++ {
				if (txDict[v1][vertList[k2]] > txDict[vertList[k2]][v1]) && (txDict[v1][vertList[k2]] >= int(threshold)) {
					txEdge = append(txEdge, v1+","+vertList[k2])
				}
				if (txDict[v1][vertList[k2]] < txDict[vertList[k2]][v1]) && (txDict[vertList[k2]][v1] >= int(threshold)) {
					txEdge = append(txEdge, vertList[k2]+","+v1)
				}
			}
		}
	}
	return txEdge
}

// 按交易序列构建有向图
func constructGraph(vertices []string, edges []string) *Graph {
	g := NewGraph()
	for _, v := range vertices {
		g.addVertex(VT(v))
	}
	for _, v1 := range edges {
		res := strings.Split(v1, ",") // 拆分字符串
		g.addEdge(VT(res[0]), VT(res[1]))
	}
	return g
}

// 查找有向图G中的强连通分量 tarjan算法
// 使用tarjan算法，一遍dfs就能求出所有强连通分量scc
type SccData struct {
	g     *Graph
	dnf   map[VT]int // 表示这个点在dfs时是第几个被搜到的
	low   map[VT]int // 表示这个点以及其子孙节点连的所有点中dfn最小的值
	stack map[int]VT // 表示当前所有可能能构成强连通分量的点
	vis   map[VT]int // 表示一个点是否在stack[ ]数组中
	deep  int        // dnf的初始值
	top   int
	scc   [][]VT // 记录所有强连通分量,输出二维数组：[[tx1,tx2...], [tx6,tx8]]
}

func InitScc(g1 *Graph) *SccData {
	sd := &SccData{
		g:     g1,
		dnf:   make(map[VT]int),
		low:   make(map[VT]int),
		stack: make(map[int]VT),
		vis:   make(map[VT]int),
		deep:  0,
		top:   0,
		scc:   make([][]VT, 0),
	}
	return sd
}

// 计算强连通分量算法
func (sd *SccData) tarjanScc(u VT) {
	sd.deep += 1
	sd.dnf[u] = sd.deep
	sd.low[u] = sd.deep
	sd.vis[u] = 1

	sd.top += 1
	sd.stack[sd.top] = u

	sz := sd.g.edgeList[u]
	for v, _ := range sz {
		if _, ok := sd.dnf[v]; !ok { // 如果顶点v还没有被搜索过
			sd.tarjanScc(v)
			if sd.low[u] > sd.low[v] {
				sd.low[u] = sd.low[v]
			}
		} else { // 如果v已经被搜索过,且v还在stack中
			if sd.vis[v] == 1 {
				if sd.low[u] > sd.dnf[v] {
					sd.low[u] = sd.dnf[v]
				}
			}
		}
	}
	if sd.dnf[u] == sd.low[u] {
		// dfn表示u点被dfs到的时间，low表示u和u所有的子树所能到达的点中dfn最小的
		// 如果两者相等，则说明u点及u点之下的所有子节点没有边是指向u的祖先的了，即u点与它的子孙节点构成了一个最大的强连通图即强连通分量
		var scc_temp []VT

		sd.vis[u] = 0 // 从栈里弹出u
		for {
			if sd.stack[sd.top] != u {
				scc_temp = append(scc_temp, sd.stack[sd.top]) // 将栈中要弹出的顶点和u放在一个强连通分量scc里
				sd.top -= 1
				sd.vis[sd.stack[sd.top]] = 0 // 从栈中弹出顶点
			} else {
				break
			}
		}
		scc_temp = append(scc_temp, sd.stack[sd.top])
		sd.top -= 1
		var scc_temp_str []VT

		// 排序scc_temp中顶点的顺序（反向取出）
		for i := len(scc_temp) - 1; i >= 0; i-- {
			scc_temp_str = append(scc_temp_str, scc_temp[i])
		}
		sd.scc = append(sd.scc, scc_temp_str) // 将一个强连通分量里的顶点列表加入scc
	}
}

// 获得所有scc强连通分量
func (sd *SccData) getSCC() [][]VT {
	return sd.scc
}

// 对图G的强连通分量进行condensation（缩合），构建输出新的图G' 【感觉缩合顶点的复杂度有点高】
// input: graph, scc
// output: g1, sccSeq(强连通分量的顺序)
func graphCondensation(g *Graph, scc [][]VT) (*Graph, map[VT][]VT) {
	sccSeq := make(map[VT][]VT)
	// 对每个强连通分量scc单独处理, 先一次把所有scc进行缩合
	for i, v := range scc {
		newV := VT("TX" + strconv.Itoa(i))
		g.addVertex(newV)
		// 对scc上的每个顶点，把所有的出边复制给缩合顶点
		// v是一个scc内的顶点集合, v1是scc中的顶点
		for _, v1 := range v {
			if _, ok := g.edgeList[v1]; ok {
				for k, _ := range g.edgeList[v1] {
					// 如果scc中顶点的出边不在缩合顶点的出边中，则增加出边
					if _, ok := g.edgeList[newV][k]; !ok {
						g.addEdge(newV, k)
					}
				}
			}
			// 获得scc顶点的出边之后，就可以从图中删除该顶点
			g.deleteVertex(v1)
			// 直接在这里对scc内的交易排序 sccSeq={TX0:[tx1,tx2,...],TX1:[tx5,...]}
			sccSeq[newV] = append(sccSeq[newV], v1)
		} // 此时图中仅包含不在scc中的顶点和缩合后的顶点

		// 对有向图中所有顶点：如果出边中包含scc中的顶点，则删除这些顶点，并添加缩合顶点的边
		for gv, _ := range g.vertList {
			// 判断有向图顶点gv的出边是否和当前scc有交集，如果有则删除gv出边中的交集顶点，并替换缩合顶点
			for gvEdge := range g.edgeList[gv] {
				var interSec []VT
				for _, item := range v {
					if gvEdge == item { // 记录交集
						interSec = append(interSec, item)
					}
				}
				if len(interSec) > 0 { // 删除交集顶点
					for _, item := range interSec {
						g.deleteEdge(gv, item)
					}
					// 增加缩合顶点
					g.addEdge(gv, newV)
				}
			}
		}
	}

	return g, sccSeq
}

// 查找入度为0的顶点集合
func findStartVertex(g *Graph) (V []VT, in_degrees map[VT]int) {
	// in_degrees := make(map[VT]int)
	gv := g.getVertices()
	// 筛选所有入度为0的顶点
	for _, u := range gv {
		in_degrees[u] = 0
	}
	for _, u := range gv {
		if len(g.edgeList[u]) > 0 {
			for v, _ := range g.edgeList[u] {
				in_degrees[v] += 1
			}
		}
	}
	for k, u := range in_degrees {
		if u == 0 {
			V = append(V, k)
		}
	}
	// 如果没有入度为0的节点，说明整个图都是强连通的
	// 此时删除图中第一个顶点的所有入边,再返回第一个顶点
	if len(V) == 0 {
		fmt.Println()
		firstVert := gv[0]
		for _, u := range gv {
			if len(g.edgeList[u]) > 0 {
				if _, ok := g.edgeList[u][firstVert]; ok { // 如果firstVert在顶点u的连接表里，则删除firstVert
					g.deleteEdge(u, firstVert)
				}
			}
		}
		V = append(V, firstVert)
	}
	return V, in_degrees
}

// 对图G'进行拓扑排序
func topoSort(g *Graph) (Seq []VT) {
	V, in_degrees := findStartVertex(g) // 查找入度为0的顶点集合
	vNum := len(g.getVertices())
	for {
		if len(V) > 0 {
			u := V[0]
			V = V[1:] // 删除V中第一个元素
			Seq = append(Seq, u)
			// 将u的所有出边的顶点中入度为0的顶点加入到V
			if g.edgeList[u] != nil {
				if len(g.edgeList[u]) > 0 {
					for v, _ := range g.edgeList[u] {
						in_degrees[v] -= 1
						if in_degrees[v] == 0 {
							V = append(V, v)
						}
					}
				}
			}
		} else {
			break
		}
	}

	if len(Seq) == vNum {
		return Seq
	} else {
		fmt.Printf("TopoSort ERROR! len seq: %d, len graph vetex: %d", len(Seq), vNum)
		return nil
	}
}

// 实现公平排序（结合拓扑排序和scc内部排序）
func finalSort(topoSeq []VT, sccSeq map[VT][]VT) (finalSeq []VT) {
	for _, item := range topoSeq {
		// 如果交易不在scc里，直接添加到最终序列
		if _, ok := sccSeq[item]; !ok {
			finalSeq = append(finalSeq, item)
		} else {
			// 如果交易在scc里，则将scc的序列加入到最终序列
			for _, v := range sccSeq[item] {
				finalSeq = append(finalSeq, v)
			}
		}
	}
	return finalSeq
}

// 排序强连通分量中的交易（寻找哈密顿回路）【暂时直接】

// Metric，计算成功排序的交易对比例（= 公平排序和发送顺序相同的交易对数量 / 发送顺序的交易对总数）
// 在协议中不会执行，计算最终结果时执行
func Metric_SuccessOrderRate(orderedSeq []VT, sendSeq []string) (sRate float32) {
	var orderedTxPair []string
	var sendTxPair []string
	matchNum := 0 // 统计成功排序的交易对数量
	// 按排序顺序计算<交易对>
	for i, _ := range orderedSeq {
		if i < len(orderedSeq)-1 {
			for j := i + 1; j < len(orderedSeq); j++ {
				orderedTxPair = append(orderedTxPair, string(orderedSeq[i])+","+string(orderedSeq[j]))
			}
		}
	}
	// 按发送顺序计算<交易对>
	for i, _ := range sendSeq {
		if i < len(sendSeq)-1 {
			for j := i + 1; j < len(sendSeq); j++ {
				sendTxPair = append(sendTxPair, sendSeq[i]+","+sendSeq[j])
			}
		}
	}
	// 计算排序<交易对>和发送<交易对>的交集数量
	for _, v1 := range orderedTxPair {
		for _, v2 := range sendTxPair {
			if v1 == v2 {
				matchNum += 1
			}
		}
	}
	// 计算成功排序的交易对比例
	sRate = float32(matchNum) / float32(len(sendTxPair))
	return sRate
}

// validator从当前交易列表中删除已经排序好的交易，计算剩余交易列表
func GetRemainTxList(txList []string, finalSeq []VT) (remainTxList []string) {
	// validator从当前交易列表里删除finalSeq中存在的交易
	// 额外新开一个数组记录剩余交易列表
	for _, v1 := range txList {
		for _, v2 := range finalSeq {
			if v1 != string(v2) {
				remainTxList = append(remainTxList, v1)
			}
		}
	}
	return remainTxList
}

// 实现Themis公平排序
// 输入：交易序列，节点数，fault，gamma
// 输出：排序后的交易序列
func FairOrder_Themis(txList [][]string, nodeNum int, fault int, gamma float32) (finalTxSeq []VT) {
	f := int(math.Floor(float64(nodeNum) / float64(fault)))
	// 1. 交易列表 构建 -> 有向图
	vertices, txListNew := findVertex(txList, nodeNum, f)
	edges := findEdges(txListNew, vertices, nodeNum, f, gamma)
	g := constructGraph(vertices, edges) // 构建有向图
	// 2. 排序有向图
	// 2.1 查找有向图中的scc
	sccdata := InitScc(g)
	startV, _ := findStartVertex(g) // 查找图中没有入度的顶点，作为对有向图遍历的起始顶点
	if len(startV) > 0 {
		for _, v := range startV {
			sccdata.tarjanScc(v)
		}
	}
	scc := sccdata.getSCC()
	// 2.2 对有向图中的scc进行condensation（缩合），构建有向无环图，并排序scc中的交易
	g1, sccSeq := graphCondensation(g, scc)
	// 2.3 计算有向无环图的拓扑排序
	topoSeq := topoSort(g1)
	// 2.4 计算最终排序(拓扑排序+scc内部排序)
	finalTxSeq = finalSort(topoSeq, sccSeq)

	return finalTxSeq
}
