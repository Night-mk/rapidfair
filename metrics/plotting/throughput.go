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
func avgThroughput(p *ThroughputPlot, interval time.Duration) plotter.XYer {
	intervals := GroupByTimeInterval(&p.startTimes, p.measurements, interval)
	return TimeAndAverage(intervals, func(m Measurement) (float64, uint64) {
		tp := m.(*types.ThroughputMeasurement)
		// 这里返回的就是TPS，commandNum/duration（时间单位是秒,s）
		return float64(tp.GetCommands()) / tp.GetDuration().AsDuration().Seconds(), 1
	})
}
