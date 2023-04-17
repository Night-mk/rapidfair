package synchronizer

import (
	"math"
	"time"
)

// ViewDuration determines the duration of a view.
// ViewDuration定义一个view的间隔时间
// The view synchronizer uses this interface to set its timeouts.
type ViewDuration interface {
	// Duration returns the duration that the next view should last.
	Duration() time.Duration
	// ViewStarted is called by the synchronizer when starting a new view.
	ViewStarted()
	// ViewSucceeded is called by the synchronizer when a view ended successfully.
	ViewSucceeded()
	// ViewTimeout is called by the synchronizer when a view timed out.
	ViewTimeout()

	// 前k个区块使用100*mean作为duration，因为genesis的延迟非常小，这样导致前k个区块的过期时间非常短
	DurationH() time.Duration
}

// NewViewDuration returns a ViewDuration that approximates the view duration based on durations of previous views.
// 构造方法返回一个view间隔，根据previous视图的持续时间来近似计算view duration
// sampleSize determines the number of previous views that should be considered.
// startTimeout determines the view duration of the first views.
// When a timeout occurs, the next view duration will be multiplied by the multiplier.
func NewViewDuration(sampleSize uint64, startTimeout, maxTimeout, multiplier float64) ViewDuration {
	return &viewDuration{
		limit: sampleSize,   // 考虑使用之前k个视图的duration作为参考
		mean:  startTimeout, // 定义第一个view的duration
		max:   maxTimeout,   // 定义最大timeout时间
		mul:   multiplier,   // 当出现timeout时，下一个view duration将乘上multiplier（延长下个view的duration）
	}
}

// viewDuration uses statistics from previous views to guess a good value for the view duration.
// It only takes a limited amount of measurements into account.
type viewDuration struct {
	mul       float64   // on failed views, multiply the current mean by this number (should be > 1)
	limit     uint64    // how many measurements should be included in mean
	count     uint64    // total number of measurements
	startTime time.Time // the start time for the current measurement
	mean      float64   // the mean view duration
	m2        float64   // sum of squares of differences from the mean
	prevM2    float64   // m2 calculated from the last period
	max       float64   // upper bound on view timeout
}

// ViewSucceeded calculates the duration of the view
// and updates the internal values used for mean and variance calculations.
// 计算view duration，并且更新用于均值、方差计算的内部变量
func (v *viewDuration) ViewSucceeded() {
	if v.startTime.IsZero() {
		return
	}

	duration := float64(time.Since(v.startTime)) / float64(time.Millisecond)
	// fmt.Println("Success View duration: ", duration)
	v.count++

	// Reset m2 occasionally such that we will pick up on changes in variance faster.
	// We store the m2 to prevM2, which will be used when calculating the variance.
	// This ensures that at least 'limit' measurements have contributed to the approximate variance.
	if v.count%v.limit == 0 {
		v.prevM2 = v.m2
		v.m2 = 0
	}

	var c float64
	if v.count > v.limit {
		c = float64(v.limit)
		// throw away one measurement
		v.mean -= v.mean / float64(c)
	} else {
		c = float64(v.count)
	}

	// Welford's algorithm
	d1 := duration - v.mean
	v.mean += d1 / c // v.mean = v.mean + (duration - v.mean)/c，其中c在count<limit时，取count的值；否则取limit的值
	d2 := duration - v.mean
	v.m2 += d1 * d2
}

// ViewTimeout should be called when a view timeout occurred. It will multiply the current mean by 'mul'.
func (v *viewDuration) ViewTimeout() {
	v.mean *= v.mul
}

// ViewStarted records the start time of a view.
func (v *viewDuration) ViewStarted() {
	v.startTime = time.Now()
}

// Duration returns the upper bound of the 95% confidence interval for the mean view duration.
func (v *viewDuration) Duration() time.Duration {
	conf := 1.96 // 95% confidence
	dev := float64(0)
	if v.count > 1 {
		c := float64(v.count)
		m2 := v.m2
		// The standard deviation is calculated from the sum of prevM2 and m2.
		if v.count >= v.limit {
			c = float64(v.limit) + float64(v.count%v.limit)
			m2 += v.prevM2
		}
		dev = math.Sqrt(m2 / c)
	}

	duration := v.mean + dev*conf
	if v.max > 0 && duration > v.max { // default的max-timout=0s, 这里就不会赋值了
		duration = v.max
	}

	// RapidFair:baseline 修改：直接用duration*10作为duration，没出错的情况一般很难超时
	if v.mean > 0 {
		duration = v.mean * 10
	}
	// RapidFair END

	return time.Duration(duration * float64(time.Millisecond))
}

func (v *viewDuration) DurationH() time.Duration {
	conf := 1.96 // 95% confidence
	dev := float64(0)
	if v.count > 1 {
		c := float64(v.count)
		m2 := v.m2
		// The standard deviation is calculated from the sum of prevM2 and m2.
		if v.count >= v.limit {
			c = float64(v.limit) + float64(v.count%v.limit)
			m2 += v.prevM2
		}
		dev = math.Sqrt(m2 / c)
	}

	duration := v.mean + dev*conf
	if v.max > 0 && duration > v.max { // default的max-timout=0s, 这里就不会赋值了
		duration = v.max
	}

	// RapidFair:baseline 修改：直接用mean*100作为duration，没出错的情况一般很难超时
	if v.mean > 0 {
		duration = v.mean * 100
	}
	// RapidFair END

	return time.Duration(duration * float64(time.Millisecond))
}
