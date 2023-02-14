package orderfairness

import (
	"fmt"
	"testing"
)

func InitGraph() *Graph {
	g := NewGraph()
	g.addVertex("tx1")
	g.addVertex("tx2")
	g.addVertex("tx3")
	g.addVertex("tx4")
	g.addVertex("tx5")
	g.addVertex("tx6")

	g.addEdge("tx1", "tx2")
	g.addEdge("tx1", "tx3")
	g.addEdge("tx2", "tx5")
	g.addEdge("tx3", "tx5")
	g.addEdge("tx5", "tx6")
	g.addEdge("tx4", "tx1")

	return g
}

// 通过测试
func TestGraph(t *testing.T) {
	g := InitGraph()
	g.Print()
	// t.Logf("%v", "add edge tx6,tx1")
	// g.addEdge("tx6", "tx1") // 增加边 （完成）
	// g.Print()

	// t.Logf("%v", "delete edge tx5,tx6") // 删除边（完成）
	// g.deleteEdge("tx5", "tx6")
	// g.Print()

	// t.Logf("%v", "add vertex tx9") // 增加顶点 （完成）
	// g.addVertex("tx9")
	// g.Print()

	// t.Logf("%v", "delete vertex tx6") // 删除顶点 （完成）
	// g.deleteVertex("tx6")
	// g.Print()

	verts := g.getVertices() // 获取所有顶点
	fmt.Print(verts)

	for _, u := range verts {
		if len(g.edgeList[u]) == 0 {
			fmt.Print(u)
		}
	}
}
