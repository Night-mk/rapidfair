package orderfairness

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
)

// building blocks
// ==============构建图和顶点类、实例==============
// =========顶点类=========
type VT string // vertex type
type Vertex struct {
	value string
}

func (v *Vertex) Print() string {
	return fmt.Sprintf("%v", v.value)
}

// =========图类=========
// 顶点：[tx1, tx2, ...] -> vertex
// 边：[tx1][tx2] -> vertex
type Graph struct {
	sync.RWMutex // 读写锁
	vertList     map[string]*Vertex
	edgeList     map[string]map[string]*Vertex
}

// 初始化图
func NewGraph() *Graph {
	g := &Graph{
		vertList: make(map[string]*Vertex),
		edgeList: make(map[string]map[string]*Vertex),
	}

	return g
}

// 增加顶点
func (g *Graph) addVertex(key string, edgeListLen int) {
	// g.Lock()
	// defer g.Unlock()
	if _, ok := g.vertList[key]; !ok { // 如果顶点不存在
		v := &Vertex{value: key}
		g.vertList[key] = v
		// 如果边的列表里没有，则要初始化
		if g.edgeList[key] == nil {
			g.edgeList[key] = make(map[string]*Vertex, edgeListLen)
		}
	}
}

// 不管是否已经存在，直接加入新顶点，并且先不初始化边
func (g *Graph) addExactVertex(key string) {
	if _, ok := g.vertList[key]; !ok { // 如果顶点不存在
		v := &Vertex{value: key}
		g.vertList[key] = v
	}
}

// 传入edgelist的最大长度进行初始化
func (g *Graph) genEdgeList(listLen int) {
	for key := range g.vertList {
		if g.edgeList[key] == nil {
			g.edgeList[key] = make(map[string]*Vertex, listLen)
		}
	}
}

// 获取顶点
func (g *Graph) getVertex(key string) (bool, *Vertex) {
	v, ok := g.vertList[key]
	return ok, v
}

// 删除顶点
func (g *Graph) deleteVertex(key string) {
	// g.Lock()
	// defer g.Unlock()
	if g.vertList[key] != nil {
		delete(g.vertList, key)
	}
	// 如果顶点在edgelist中，则删除edgelist中的内容
	if g.edgeList[key] != nil {
		delete(g.edgeList, key)
	}
	// 如果顶点key在其他顶点的列表里，则也删除
	for k, v := range g.edgeList {
		if _, ok := v[key]; ok {
			delete(g.edgeList[k], key)
		}
	}
}

// 增加边（有向边）
func (g *Graph) addEdge(v1 string, v2 string) {
	// g.Lock()
	// defer g.Unlock()
	if g.edgeList == nil {
		g.edgeList = make(map[string]map[string]*Vertex)
	}
	if _, ok := g.vertList[v1]; !ok {
		g.vertList[v1] = &Vertex{value: v1}
	}
	if _, ok1 := g.vertList[v2]; !ok1 {
		g.vertList[v2] = &Vertex{value: v2}
	}

	if g.edgeList[v1] == nil {
		g.edgeList[v1] = make(map[string]*Vertex)
	}
	g.edgeList[v1][v2] = &Vertex{value: v2}
}

// 增加一定存在顶点的有向边
func (g *Graph) addExactEdge(v1 string, v2 string) {
	g.edgeList[v1][v2] = &Vertex{value: v2}
}

// 删除边
func (g *Graph) deleteEdge(v1 string, v2 string) {
	// g.Lock()
	// defer g.Unlock()
	if _, ok := g.edgeList[v1][v2]; ok {
		delete(g.edgeList[v1], v2)
	}
}

// 获取图的所有顶点key
func (g *Graph) getVertices() []string {
	// g.Lock()
	// defer g.Unlock()
	verts := make([]string, 0, len(g.vertList))
	for k := range g.vertList {
		verts = append(verts, k)
	}
	return verts
}

// 获取图中顶点v的边列表
func (g *Graph) getEdges(v string) []string {
	// g.Lock()
	// defer g.Unlock()
	edges := make([]string, 0, len(g.edgeList[v]))
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

// 使用string.Builder拼接字符串
func builderConcat(str1 string, str2 string) string {
	var builder strings.Builder
	builder.WriteString(str1)
	builder.WriteString(str2)
	// for i := 0; i < n; i++ {
	// 	builder.WriteString(str)
	// }
	return builder.String()
}

// 公平排序算法部分
// =========利用交易序列构建有向图并排序=========
// 1. 从n-f个交易序列中获取顶点，查找数量超过n-2f的顶点加入有向图
func findVertex(txList [][]string, n int, f int) ([]string, [][]string, *Graph) {
	g := NewGraph()
	vertNum := make(map[string]int)
	var vertList []string
	threshold := n - 2*f

	failVerts := make(map[string]int)
	txListNew := make([][]string, len(txList))

	// 计算交易出现的数量
	for i := 0; i < len(txList); i++ {
		// for _, v1 := range txList {
		for j := 0; j < len(txList[i]); j++ {
			// for _, v2 := range v1 {
			if _, ok := vertNum[txList[i][j]]; !ok {
				vertNum[txList[i][j]] = 1
			} else {
				vertNum[txList[i][j]] += 1
			}
		}
	}
	// 计算能加入有向图的交易顶点
	for k, v := range vertNum {
		if v >= threshold {
			vertList = append(vertList, k)
			// 将交易序列的长度作为图中每个顶点的edgelist长度初始化
			//（看看能不能优化写入edge时效率低的问题）
			g.addVertex(k, len(txList[0])) // 这里直接将顶点加入有向图
			// g.addExactVertex(k) // 这里仅在图中加入顶点但是不创建顶点的edgelist
		} else {
			failVerts[k] = 0
		}
	}
	// 将交易序列的长度作为图中每个顶点的edgelist长度初始化
	//（看看能不能优化写入edge时效率低的问题）
	// g.genEdgeList(len(txList[0]))

	// 构建新的交易序列
	// 删除交易序列中不能加入图的交易
	if len(failVerts) > 0 {
		for i := 0; i < len(txList); i++ {
			// for k1, v1 := range txList {
			txListNew[i] = make([]string, 0)
			for j := 0; j < len(txList[i]); j++ {
				// for _, v2 := range v1 {
				if _, ok := failVerts[txList[i][j]]; !ok { // 把不在fail中的交易加入新txList
					txListNew[i] = append(txListNew[i], txList[i][j])
				}
			}
		}
	} else {
		return vertList, txList, g
	}

	return vertList, txListNew, g
}

// 计算交易有向图的边列表
// 输入的txList只包含足够数量的交易
// 由于这里计算了顶点和边，可以直接构建有向图，不用分开计算
/*
func findEdges(txList [][]string, vertList []string, n int, f int, gamma float32) *Graph {
	g := NewGraph()
	//构建M*M大小的二维字典
	start0 := time.Now()
	txDict := make(map[string]map[string]int)
	for _, i := range vertList {
		// 图增加顶点
		g.addVertex(string(i))
		txDict[i] = make(map[string]int)
		for _, j := range vertList {
			txDict[i][j] = 0
		}
	}
	elapsed0 := time.Since(start0)
	fmt.Println("  Execution time (construct dict):", elapsed0)

	// 计算所有交易序列中，交易对(m,m')的数量
	start1 := time.Now() // 获取当前时间
	for _, x := range txList {
		for k1, v1 := range x {
			if k1 < len(x)-1 {
				for k2 := k1 + 1; k2 < len(x); k2++ {
					txDict[v1][x[k2]] += 1
				}
			}
		}
	}
	elapsed1 := time.Since(start1)
	fmt.Println("  Execution time (tx pair):", elapsed1)

	// 筛选edge M[m][m']加入有向图
	start2 := time.Now()
	threshold := float32(n)*(float32(1)-gamma) + float32(f) + 1
	// 输出数组["tx1;tx2","tx1;tx3",...]
	for k1, v1 := range vertList {
		if k1 < len(vertList)-1 {
			for k2 := k1 + 1; k2 < len(vertList); k2++ {
				if (txDict[v1][vertList[k2]] > txDict[vertList[k2]][v1]) && (txDict[v1][vertList[k2]] >= int(threshold)) {
					// txEdge = append(txEdge, v1+";"+vertList[k2])
					g.addEdge(string(v1), string(vertList[k2]))
				}
				if (txDict[v1][vertList[k2]] < txDict[vertList[k2]][v1]) && (txDict[vertList[k2]][v1] >= int(threshold)) {
					// txEdge = append(txEdge, vertList[k2]+";"+v1)
					g.addEdge(string(vertList[k2]), string(v1))
				}
			}
		}
	}
	elapsed2 := time.Since(start2)
	fmt.Println("  Execution time (add edge):", elapsed2)

	// return txEdge, g
	return g
}
*/

// 慢的核心问题是map读写太慢了，特别是读
func findEdges(txList [][]string, vertList []string, n int, f int, gamma float32, g *Graph) *Graph {
	// g := NewGraph()
	// 构建M*M大小的二维字典（初始化其实不必要）
	// txDict := make(map[string]int, len(vertList)*len(vertList))
	txDict := make(map[[2]string]int, len(vertList)*len(vertList))
	// 计算所有交易序列中，交易对(m,m')的数量
	// 使用下标而不是用range进行循环测试下性能变化
	for i := 0; i < len(txList); i++ {
		for j := 0; j < len(txList[i])-1; j++ {
			// if j < len(txList[i])-1 {
			for k2 := j + 1; k2 < len(txList[i]); k2++ {
				pair := [2]string{txList[i][j], txList[i][k2]}
				txDict[pair] += 1
			}
			// }
		}
	}

	// 筛选edge M[m][m']加入有向图
	threshold := float32(n)*(float32(1)-gamma) + float32(f) + 1
	// 输出数组["tx1;tx2","tx1;tx3",...]
	for k1 := 0; k1 < len(vertList)-1; k1++ {
		// lvt := len(vertList)
		// if k1 < lvt-1 {
		for k2 := k1 + 1; k2 < len(vertList); k2++ {
			pair1 := [2]string{vertList[k1], vertList[k2]}
			pair2 := [2]string{vertList[k2], vertList[k1]}
			w1 := txDict[pair1]
			w2 := txDict[pair2]
			if (w1 > w2) && (w1 >= int(threshold)) {
				g.addExactEdge(vertList[k1], vertList[k2])
			}
			if (w1 < w2) && (w2 >= int(threshold)) {
				g.addExactEdge(vertList[k2], vertList[k1])
			}
		}
		// }
	}

	return g
}

func findEdges_Measure(txList [][]string, vertList []string, n int, f int, gamma float32, g *Graph) *Graph {
	// g := NewGraph()
	// 构建M*M大小的二维字典（初始化其实不必要）
	// txDict := make(map[string]int, len(vertList)*len(vertList))
	txDict := make(map[[2]string]int, len(vertList)*len(vertList))
	// 计算所有交易序列中，交易对(m,m')的数量
	start1 := time.Now() // 获取当前时间
	execNum := 0
	// 使用下标而不是用range进行循环测试下性能变化
	for i := 0; i < len(txList); i++ {
		for j := 0; j < len(txList[i]); j++ {
			if j < len(txList[i])-1 {
				for k2 := j + 1; k2 < len(txList[i]); k2++ {
					execNum += 1
					pair := [2]string{txList[i][j], txList[i][k2]}
					txDict[pair] += 1
				}
			}
		}
	}
	// for _, x := range txList { // 对每个交易序列，总共n-f个
	// 	for k1, v1 := range x {
	// 		if k1 < len(x)-1 {
	// 			for k2 := k1 + 1; k2 < len(x); k2++ {
	// 				execNum += 1
	// 				// txDict[v1+x[k2]] += 1
	// 				pair := [2]string{v1, x[k2]}
	// 				txDict[pair] += 1
	// 			}
	// 		}
	// 	}
	// }
	elapsed1 := time.Since(start1)
	fmt.Println("  Execution time (tx pair):", elapsed1)
	fmt.Println("  execNum:", execNum)

	// 筛选edge M[m][m']加入有向图
	start2 := time.Now()
	threshold := float32(n)*(float32(1)-gamma) + float32(f) + 1
	// 输出数组["tx1;tx2","tx1;tx3",...]
	execNum1 := 0
	// for k1, v1 := range vertList {
	for k1 := 0; k1 < len(vertList); k1++ {
		lvt := len(vertList)
		if k1 < lvt-1 {
			for k2 := k1 + 1; k2 < lvt; k2++ {
				execNum1 += 1
				pair1 := [2]string{vertList[k1], vertList[k2]}
				pair2 := [2]string{vertList[k2], vertList[k1]}
				w1 := txDict[pair1]
				w2 := txDict[pair2]
				if (w1 > w2) && (w1 >= int(threshold)) {
					g.addExactEdge(vertList[k1], vertList[k2])
				}
				if (w1 < w2) && (w2 >= int(threshold)) {
					g.addExactEdge(vertList[k2], vertList[k1])
				}
			}
		}
	}
	elapsed2 := time.Since(start2)
	fmt.Println("  Execution time (add edge):", elapsed2)
	fmt.Println("  execNum1:", execNum1)

	return g
}

func testCmap(txList [][]string, vertList []string, n int, f int, gamma float32, g *Graph) {
	// 尝试下使用cmap，优化map读写？
	txDict := cmap.New[int]()
	// for _, i := range vertList {
	// 	for _, j := range vertList {
	// 		txDict.Set(i+j, 0)
	// 	}
	// }

	// 计算所有交易序列中，交易对(m,m')的数量
	start1 := time.Now() // 获取当前时间
	execNum := 0
	// quitNum := len(txList[0]) * (len(txList[0]) - 1) / 2
	// quit := make(chan int, len(txList))
	for _, x := range txList { // 对每个交易序列，总共n-f个
		wg := sync.WaitGroup{} // 使用waitGroup试试并发
		lx := len(x)
		wg.Add(lx)
		// 尝试做一下并发读写，对n-f列表做并发
		// go func(x []string, quit chan int) {
		for k1, v1 := range x {
			go func(x []string, k1 int, v1 string, lx int) {
				if k1 < lx-1 {
					for k2 := k1 + 1; k2 < lx; k2++ {
						execNum += 1
						// value, _ := txDict.Get(v1 + x[k2])
						// txDict.Set(v1+x[k2], value+1)
						if value, ok := txDict.Get(v1 + x[k2]); !ok {
							txDict.Set(v1+x[k2], 1)
						} else {
							txDict.Set(v1+x[k2], value+1)
						}
					}
				}
				wg.Done()
			}(x, k1, v1, lx)
		}
		wg.Wait()
		// quit <- 1
		// }(x, quit)
	}
	// for j := 0; j < len(txList); j++ {
	// 	<-quit
	// }
	// close(quit)
	elapsed1 := time.Since(start1)
	// fmt.Println("  Execution time (tx pair with preprocess)——cmap:", elapsed1)
	fmt.Println("  Execution time (tx pair)——cmap:", elapsed1)
	fmt.Println("  execNum:", execNum)

	// 筛选edge M[m][m']加入有向图
	start2 := time.Now()
	threshold := float32(n)*(float32(1)-gamma) + float32(f) + 1
	// 输出数组["tx1;tx2","tx1;tx3",...]
	execNum1 := 0
	for k1, v1 := range vertList {
		if k1 < len(vertList)-1 {
			for k2 := k1 + 1; k2 < len(vertList); k2++ {
				execNum1 += 1
				w1, _ := txDict.Get(v1 + vertList[k2])
				w2, _ := txDict.Get(vertList[k2] + v1)
				if (w1 > w2) && (w1 >= int(threshold)) {
					w1 = 0
					// g.addExactEdge(v1, vertList[k2])
				}
				if (w1 < w2) && (w2 >= int(threshold)) {
					w2 = 0
					// g.addExactEdge(vertList[k2], v1)
				}
			}
		}
	}
	elapsed2 := time.Since(start2)
	fmt.Println("  Execution time (read map)——cmap:", elapsed2)
	fmt.Println("  execNum1:", execNum1)

}

func testMap(txList [][]string, vertList []string, n int, f int, gamma float32, g *Graph) {
	// g := NewGraph()
	// 构建M*M大小的二维字典（初始化其实不必要）
	// txDict := make(map[string]int, len(vertList)*len(vertList))
	// 试试不使用连接string的方法来做key
	txDict := make(map[[2]string]int, len(vertList)*len(vertList))
	// 计算所有交易序列中，交易对(m,m')的数量
	start1 := time.Now() // 获取当前时间
	execNum := 0
	// 使用下标而不是用range进行循环测试下性能变化
	for i := 0; i < len(txList); i++ {
		for j := 0; j < len(txList[i]); j++ {
			if j < len(txList[i])-1 {
				for k2 := j + 1; k2 < len(txList[i]); k2++ {
					execNum += 1
					pair := [2]string{txList[i][j], txList[i][k2]}
					txDict[pair] += 1
				}
			}
		}
	}
	// for _, x := range txList { // 对每个交易序列，总共n-f个
	// 	for k1, v1 := range x {
	// 		if k1 < len(x)-1 {
	// 			for k2 := k1 + 1; k2 < len(x); k2++ {
	// 				execNum += 1
	// 				pair := [2]string{v1, x[k2]}
	// 				txDict[pair] += 1
	// 			}
	// 		}
	// 	}
	// }
	elapsed1 := time.Since(start1)
	fmt.Println("  Execution time (with for not range):", elapsed1)
	fmt.Println("  execNum:", execNum)

	// 筛选edge M[m][m']加入有向图
	start2 := time.Now()
	threshold := float32(n)*(float32(1)-gamma) + float32(f) + 1
	// 输出数组["tx1;tx2","tx1;tx3",...]
	// txEdges := make(map[string]int, len(vertList)*len(vertList))
	execNum1 := 0
	for k1, v1 := range vertList {
		lvt := len(vertList)
		if k1 < lvt-1 {
			for k2 := k1 + 1; k2 < lvt; k2++ {
				execNum1 += 1
				w1 := txDict[[2]string{v1, vertList[k2]}]
				w2 := txDict[[2]string{vertList[k2], v1}]
				// w1 := txDict[v1+vertList[k2]]
				// w2 := txDict[vertList[k2]+v1]
				if (w1 > w2) && (w1 >= int(threshold)) {
					w1 = 0
					// txEdges[v1+vertList[k2]] = w1
				}
				if (w1 < w2) && (w2 >= int(threshold)) {
					w2 = 0
					// txEdges[vertList[k2]+v1] = w2
				}
			}
		}
	}
	elapsed2 := time.Since(start2)
	fmt.Println("  Execution time (read map):", elapsed2)
	fmt.Println("  execNum1:", execNum1)
}

// 按交易序列构建有向图
// constructGraph这里有对交易的类型做了区别
/*
func constructGraph(vertices []string, edges []string) *Graph {
	g := NewGraph()
	for _, v := range vertices {
		g.addVertex(v)
	}
	for _, v1 := range edges {
		res := strings.Split(v1, ";") // 拆分字符串
		g.addEdge(res[0], res[1])
	}
	return g
}
*/

// 查找有向图G中的强连通分量 tarjan算法
// 使用tarjan算法，一遍dfs就能求出所有强连通分量scc
type SccData struct {
	g     *Graph
	dnf   map[string]int // 表示这个点在dfs时是第几个被搜到的
	low   map[string]int // 表示这个点以及其子孙节点连的所有点中dfn最小的值
	stack map[int]string // 表示当前所有可能能构成强连通分量的点
	vis   map[string]int // 表示一个点是否在stack[ ]数组中
	deep  int            // dnf的初始值
	top   int
	scc   [][]string // 记录所有强连通分量,输出二维数组：[[tx1,tx2...], [tx6,tx8]]
}

func InitScc(g1 *Graph) *SccData {
	sd := &SccData{
		g:     g1,
		dnf:   make(map[string]int), // 表示这个点在DFS时是第几个被搜到的
		low:   make(map[string]int), // 表示这个点以及其子孙节点连的所有点中dfn最小的值
		stack: make(map[int]string), // 表示当前所有可能能构成强连通分量的点
		vis:   make(map[string]int), // 表示一个点是否在stack[]数组中
		deep:  0,                    // dnf的初始值
		top:   0,
		scc:   make([][]string, 0), // 记录强连通分量
	}
	return sd
}

// 计算强连通分量算法
func (sd *SccData) tarjanScc(u string) {
	sd.deep += 1
	sd.dnf[u] = sd.deep
	sd.low[u] = sd.deep // 记录顶点和其子孙顶点连接的所有顶点中最小的dfn
	sd.vis[u] = 1       // 访问过顶点u记录=1

	sd.top += 1
	sd.stack[sd.top] = u // 将没有访问过的顶点入栈

	sz := sd.g.edgeList[u] // 获取节点u的所有邻接顶点 sz=map[string]*Vertex
	for v := range sz {
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
		var scc_temp []string

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
		// 将强连通分量数量>1的顶点列表加入scc
		if len(scc_temp) > 1 {
			var scc_temp_str []string
			// 排序scc_temp中顶点的顺序（反向取出）
			for i := len(scc_temp) - 1; i >= 0; i-- {
				scc_temp_str = append(scc_temp_str, scc_temp[i])
			}
			sd.scc = append(sd.scc, scc_temp_str) // 将一个强连通分量里的顶点列表加入scc
		}
	}
}

// 获得所有scc强连通分量
func (sd *SccData) getSCC() [][]string {
	return sd.scc
}

// 对图G的强连通分量进行condensation（缩合），构建输出新的图G' 【感觉缩合顶点的复杂度有点高】
// input: graph, scc
// output: g1, sccSeq(强连通分量的顺序)
func graphCondensation(g *Graph, scc [][]string, txListLen int) (*Graph, map[string][]string) {
	sccSeq := make(map[string][]string)
	// 对每个强连通分量scc单独处理, 先一次把所有scc进行缩合
	for i := 0; i < len(scc); i++ {
		// for i, v := range scc {
		newV := "TX" + strconv.Itoa(i)
		g.addVertex(newV, txListLen)
		// 对scc上的每个顶点，把所有的出边复制给缩合顶点
		// v是一个scc内的顶点集合, v1是scc中的顶点
		for j := 0; j < len(scc[i]); j++ {
			// for _, v1 := range v {
			// if _, ok := g.edgeList[v1]; ok {
			if _, ok := g.edgeList[scc[i][j]]; ok {
				for k := range g.edgeList[scc[i][j]] {
					// 如果scc中顶点的出边不在缩合顶点的出边中，则增加出边
					if _, ok := g.edgeList[newV][k]; !ok {
						// g.addEdge(newV, k)
						g.addExactEdge(newV, k)
					}
				}
			}
			// 获得scc顶点的出边之后，就可以从图中删除该顶点（删除一个节点也很耗时）
			g.deleteVertex(scc[i][j])
			// 直接在这里对scc内的交易排序 sccSeq={TX0:[tx1,tx2,...],TX1:[tx5,...]}
			sccSeq[newV] = append(sccSeq[newV], scc[i][j])
		} // 此时图中仅包含不在scc中的顶点和缩合后的顶点

		// 对有向图中所有顶点：如果出边中包含scc中的顶点，则删除这些顶点，并添加缩合顶点的边
		for gv := range g.vertList {
			// 判断有向图顶点gv的出边是否和当前scc有交集，如果有则删除gv出边中的交集顶点，并替换缩合顶点
			for gvEdge := range g.edgeList[gv] {
				var interSec []string
				for k := 0; k < len(scc[i]); k++ {
					// for _, item := range scc[i] {
					if gvEdge == scc[i][k] { // 记录交集
						interSec = append(interSec, scc[i][k])
					}
				}
				if len(interSec) > 0 { // 删除交集顶点
					for k := 0; k < len(interSec); k++ {
						// for _, item := range interSec {
						g.deleteEdge(gv, interSec[k])
					}
					// 增加缩合顶点
					// g.addEdge(gv, newV)
					g.addExactEdge(gv, newV)
				}
			}
		}
	}

	return g, sccSeq
}

// 查找入度为0的顶点集合
func findStartVertex(g *Graph) (V []string, in_degrees map[string]int) {
	in_degrees = make(map[string]int)
	gv := g.getVertices()
	// fmt.Println("find start v len(v): ", len(gv))
	// 筛选所有入度为0的顶点
	// for _, u := range gv {
	for i := 0; i < len(gv); i++ {
		in_degrees[gv[i]] = 0
	}
	// for _, u := range gv {
	for j := 0; j < len(gv); j++ {
		if len(g.edgeList[gv[j]]) > 0 {
			for v := range g.edgeList[gv[j]] {
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
		firstVert := gv[0]
		for i := 0; i < len(gv); i++ {
			// for _, u := range gv {
			if len(g.edgeList[gv[i]]) > 0 {
				if _, ok := g.edgeList[gv[i]][firstVert]; ok { // 如果firstVert在顶点u的连接表里，则删除firstVert
					g.deleteEdge(gv[i], firstVert)
				}
			}
		}
		V = append(V, firstVert)
	}
	return V, in_degrees
}

// 对图G'进行拓扑排序
func topoSort(g *Graph) (Seq []string) {
	V, in_degrees := findStartVertex(g) // 查找入度为0的顶点集合
	// fmt.Println("Verts in=0: ", V)
	// fmt.Println("in_degrees: ", in_degrees)
	vNum := len(g.getVertices()) // 记录总顶点数量
	for {
		if len(V) > 0 {
			u := V[0]
			V = V[1:] // 删除V中第一个元素
			Seq = append(Seq, u)
			// 将u的所有出边的顶点中入度为0的顶点加入到V
			if _, ok := g.edgeList[u]; ok {
				for v := range g.edgeList[u] {
					in_degrees[v] -= 1
					if in_degrees[v] == 0 {
						V = append(V, v)
					}
				}
			}
		} else {
			break
		}
	}

	if len(Seq) == vNum { // 拓扑排序的顶点数 = 总顶点数量 时，才输出
		return Seq
	} else {
		fmt.Printf("TopoSort ERROR! len seq: %d, len graph vetex: %d\n", len(Seq), vNum)
		return nil
	}
}

// 实现公平排序（结合拓扑排序和scc内部排序）
func finalSort(topoSeq []string, sccSeq map[string][]string) (finalSeq []string) {
	for i := 0; i < len(topoSeq); i++ {
		// for _, item := range topoSeq {
		// 如果交易不在scc里，直接添加到最终序列
		if _, ok := sccSeq[topoSeq[i]]; !ok {
			finalSeq = append(finalSeq, topoSeq[i])
		} else {
			// 如果交易在scc里，则将scc的序列加入到最终序列
			finalSeq = append(finalSeq, sccSeq[topoSeq[i]]...)
			// for _, v := range sccSeq[item] {
			// 	finalSeq = append(finalSeq, v)
			// }
		}
	}
	return finalSeq
}

// 排序强连通分量中的交易（寻找哈密顿回路）【暂时直接】

// Metric，计算成功排序的交易对比例（= 公平排序和发送顺序相同的交易对数量 / 发送顺序的交易对总数）
// 在协议中不会执行，计算最终结果时执行
func Metric_SuccessOrderRate(orderedSeq []string, sendSeq []string) (sRate float32) {
	var orderedTxPair []string
	var sendTxPair []string
	matchNum := 0 // 统计成功排序的交易对数量
	// 按排序顺序计算<交易对>
	for i := range orderedSeq {
		if i < len(orderedSeq)-1 {
			for j := i + 1; j < len(orderedSeq); j++ {
				orderedTxPair = append(orderedTxPair, string(orderedSeq[i])+","+string(orderedSeq[j]))
			}
		}
	}
	// 按发送顺序计算<交易对>
	for i := range sendSeq {
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
func GetRemainTxList(txList []string, finalSeq []string) (remainTxList []string) {
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
func FairOrder_Themis_Measure(txList [][]string, nodeNum int, gamma float32) (finalTxSeq []string) {
	// 计算fault replica数量, Themis中：f<=(n-1)(2*gamma-1)/4
	f := int(math.Floor((float64(nodeNum-1) * float64(2*gamma-1)) / float64(4)))
	// fmt.Println("fault replica num: ", f)
	// fmt.Printf("quorum len: %d, txList len: %d\n", len(txList), len(txList[0]))
	// 1. 交易列表 构建 -> 有向图
	start1 := time.Now()
	vertices, txListNew, g := findVertex(txList, nodeNum, f)
	txListLen := len(txList[0])
	elapsed1 := time.Since(start1)
	fmt.Println("Execution time (findVertex):", elapsed1)
	// fmt.Println("=====fair order part=====")
	// fmt.Println("vertices len: ", len(vertices))
	// fmt.Println("ordered txs: ", txListNew)

	start2 := time.Now()
	g = findEdges_Measure(txListNew, vertices, nodeNum, f, gamma, g) // 构建有向图
	// edges := findEdges(txListNew, vertices, nodeNum, f, gamma)
	elapsed2 := time.Since(start2)
	fmt.Println("Execution time (findEdges+constructGraph):", elapsed2)
	// fmt.Println("edges: ", edges)

	// testCmap(txListNew, vertices, nodeNum, f, gamma, g)
	// testMap(txListNew, vertices, nodeNum, f, gamma, g)

	// start3 := time.Now()
	// g := constructGraph(vertices, edges)
	// elapsed3 := time.Since(start3)
	// fmt.Println("Execution time (constructGraph):", elapsed3)

	// 2. 排序有向图
	// 2.1 查找有向图中的scc
	start4 := time.Now()
	sccdata := InitScc(g)
	startV, _ := findStartVertex(g) // 查找图中没有入度的顶点，作为对有向图遍历的起始顶点
	if len(startV) > 0 {
		for _, v := range startV {
			sccdata.tarjanScc(v)
		}
	}
	scc := sccdata.getSCC()
	elapsed4 := time.Since(start4)
	fmt.Println("Execution time (scc):", elapsed4)
	fmt.Println("check scc len: ", len(scc))
	// fmt.Println("all scc: ", scc)

	// 2.2 对有向图中的scc进行condensation（缩合），构建有向无环图，并排序scc中的交易
	start5 := time.Now()
	g1, sccSeq := graphCondensation(g, scc, txListLen)
	elapsed5 := time.Since(start5)
	fmt.Println("Execution time (graphCondensation):", elapsed5)
	// fmt.Println("scc seq: ", sccSeq)

	// 2.3 计算有向无环图的拓扑排序
	start6 := time.Now()
	topoSeq := topoSort(g1)
	elapsed6 := time.Since(start6)
	fmt.Println("Execution time (topoSort):", elapsed6)

	// 2.4 计算最终排序(拓扑排序+scc内部排序)
	start7 := time.Now()
	finalTxSeq = finalSort(topoSeq, sccSeq)
	elapsed7 := time.Since(start7)
	fmt.Println("Execution time (finalSort):", elapsed7)
	// fmt.Println("finalTxSeq: ", finalTxSeq)

	return finalTxSeq
}

// 使用拆分的思想将交易拆分为batch-size=25来分别处理（分组应该会损伤公平性的，不采用）
func FairOrder_Themis_Split(txList [][]string, nodeNum int, gamma float32) (finalTxSeq []string) {
	txListLen := len(txList[0])
	fmt.Println("txlist nodes=", len(txList))
	lowBatch := 25
	groupNum := txListLen / lowBatch
	txListGroup := make([][][]string, groupNum)
	var start, end, i int
	for i = 0; i < groupNum; i++ {
		end = (i + 1) * lowBatch
		txListGroup[i] = make([][]string, 0)
		for j := 0; j < len(txList); j++ {
			if i != groupNum {
				txListGroup[i] = append(txListGroup[i], txList[j][start:end])
			} else {
				txListGroup[i] = append(txListGroup[i], txList[j][start:])
			}
		}
		start = (i + 1) * lowBatch
	}

	for _, v := range txListGroup {
		fmt.Println("txlists = ", v)
	}

	return finalTxSeq
}

func FairOrder_Themis(txList [][]string, nodeNum int, gamma float32) (finalTxSeq []string) {
	// 计算fault replica数量, Themis中：f<=(n-1)(2*gamma-1)/4
	f := int(math.Floor((float64(nodeNum-1) * float64(2*gamma-1)) / float64(4)))
	// 1. 交易列表 构建 -> 有向图
	// 1.1 构建有向图（增加顶点）
	vertices, txListNew, g := findVertex(txList, nodeNum, f)
	txListLen := len(txList[0])

	// 1.1 构建有向图（增加边）
	g = findEdges(txListNew, vertices, nodeNum, f, gamma, g)

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
	g1, sccSeq := graphCondensation(g, scc, txListLen)

	// 2.3 计算有向无环图的拓扑排序
	topoSeq := topoSort(g1)

	// 2.4 计算最终排序(拓扑排序+scc内部排序)
	finalTxSeq = finalSort(topoSeq, sccSeq)

	return finalTxSeq
}
