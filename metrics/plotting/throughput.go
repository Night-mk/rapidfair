package plotting

import (
	"fmt"
	"path"
	"time"

	"github.com/relab/hotstuff/metrics/types"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
)

// ThroughputPlot is a plotter that plots throughput vs time.
// 绘制TPS和time的关系图
type ThroughputPlot struct {
	startTimes   StartTimes
	measurements MeasurementMap
}

// NewThroughputPlot returns a new throughput plotter.
func NewThroughputPlot() ThroughputPlot {
	return ThroughputPlot{
		startTimes:   NewStartTimes(),
		measurements: NewMeasurementMap(),
	}
}

// Add adds a measurement to the plotter.
// 实现./metrics/plotting/reader.go的Plotter interface（Add()方法）
// 在./metrics/plotting/reader.go中被read()调用
func (p *ThroughputPlot) Add(measurement any) {
	p.startTimes.Add(measurement)

	throughput, ok := measurement.(*types.ThroughputMeasurement)
	if !ok {
		return
	}

	if throughput.GetEvent().GetClient() {
		// ignoring client events
		return
	}

	id := throughput.GetEvent().GetID()
	// 调用./metrics/plotting/helper.go的Add()方法
	// 加入的throughput应该是*types.ThroughputMeasurement
	p.measurements.Add(id, throughput)
}

// PlotAverage plots the average throughput of all replicas at specified time intervals.
// filename是输出文件的文件名
func (p *ThroughputPlot) PlotAverage(filename string, measurementInterval time.Duration) (err error) {
	const (
		xlabel = "Time (seconds)"
		ylabel = "Throughput (commands/second)"
	)
	if path.Ext(filename) == ".csv" {
		return CSVPlot(filename, []string{xlabel, ylabel}, func() plotter.XYer {
			return avgThroughput(p, measurementInterval)
		})
	}
	// GonumPlot构建绘图的大小，xy轴的标题，label等
	return GonumPlot(filename, xlabel, ylabel, func(plt *plot.Plot) error {
		// AddLinePoints绘制折线图，plt是plot.New()构建的对象
		if err := plotutil.AddLinePoints(plt, avgThroughput(p, measurementInterval)); err != nil {
			return fmt.Errorf("failed to add line plot: %w", err)
		}
		return nil
	})
}

// 返回绘制的X，Y轴的值
// 这里interval在配置文件里定义，一般是取1 second
func avgThroughput(p *ThroughputPlot, interval time.Duration) plotter.XYer {
	fmt.Println("call avgThroughput")
	// Themis实现在GroupByTimeInterval函数计算里有问题？
	intervals := GroupByTimeInterval(&p.startTimes, p.measurements, interval)

	return TimeAndAverage(intervals, func(m Measurement) (float64, uint64) {
		// tp将m强制转为ThroughputMeasurement类型
		tp := m.(*types.ThroughputMeasurement)
		// 这里返回的就是TPS，commandNum/duration（时间单位是秒,s），后面的1表示
		return float64(tp.GetCommands()) / tp.GetDuration().AsDuration().Seconds(), 1
	})
}

// RapidFair
// 返回throughput Measurement的 区块延迟=间隔时间/提交区块的数量=duration/Commits（返回单位为毫秒ms）
// 如果计算hotstuff的交易延迟 = 区块延迟*3
// 原本的实现只能计算client的端到端延迟
func GetBlockLatency(m Measurement) float64 {
	tp := m.(*types.ThroughputMeasurement)
	if tp.GetCommits() == uint64(0) {
		return tp.GetDuration().AsDuration().Seconds() * 1000
	}
	return tp.GetDuration().AsDuration().Seconds() * 1000 / float64(tp.GetCommits())
}
