package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/relab/hotstuff/internal/proto/orchestrationpb"
	"github.com/relab/hotstuff/metrics/plotting"
	_ "github.com/relab/hotstuff/metrics/types"
)

var (
	// 增加srcPath的命令行flage
	expdata             = flag.String("expdatapath", "", "File to read exp measurement.json")
	interval            = flag.Duration("interval", time.Second, "Length of time interval to group measurements by.")
	latency             = flag.String("latency", "", "File to save latency plot to.")
	throughput          = flag.String("throughput", "", "File to save throughput plot to.")
	throughputVSLatency = flag.String("throughputvslatency", "", "File to save throughput vs latency plot to.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] [path to measurements.json]\n", os.Args[0])
		flag.PrintDefaults()
	}
	// 命令行flag的解析函数，会在碰到第一个非flag命令行参数时停止解析
	flag.Parse()

	// srcPath := flag.Arg(0)
	srcPath := *expdata
	fmt.Println("path: ", srcPath)
	if srcPath == "" { // 如果输入文件为空时，才执行usage函数
		flag.Usage()
		os.Exit(1)
	}

	file, err := os.Open(srcPath)
	if err != nil {
		// 测试是否读取失败
		fmt.Printf("open file err: %v \n", err)
		log.Fatalln(err)
	}

	latencyPlot := plotting.NewClientLatencyPlot()
	throughputPlot := plotting.NewThroughputPlot()
	throughputVSLatencyPlot := plotting.NewThroughputVSLatencyPlot()

	// reader对象将json文件中读取的对应内容写入对应的Reader.plotter数组中
	reader := plotting.NewReader(file, &latencyPlot, &throughputPlot, &throughputVSLatencyPlot)
	if err := reader.ReadAll(); err != nil {
		log.Fatalln(err)
	}

	fmt.Println("save file name(latency): ", *latency)
	fmt.Println("save file name(throughput): ", *throughput)
	fmt.Println("save file name(throughputVSLatency): ", *throughputVSLatency)

	if *latency != "" {
		if err := latencyPlot.PlotAverage(*latency, *interval); err != nil {
			log.Fatalln(err)
		}
	}

	if *throughput != "" {
		if err := throughputPlot.PlotAverage(*throughput, *interval); err != nil {
			log.Fatalln(err)
		}
	}

	if *throughputVSLatency != "" {
		if err := throughputVSLatencyPlot.PlotAverage(*throughputVSLatency, *interval); err != nil {
			log.Fatalln(err)
		}
	}
}
