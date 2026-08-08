package ffmpeg

import (
	"math"
	"sort"
	"sync"
	"time"
)

// PredictorConfig controls a RollingPredictor. Zero values select conservative
// defaults suitable for process-duration measurements.
type PredictorConfig struct {
	Window     time.Duration
	Capacity   int
	MaxKeys    int
	MinSamples int
}

// OperationSample is one completed unit of work. Work is deliberately
// unitless: callers may use audio seconds, bytes, records, or another positive
// measure that explains operation cost.
type OperationSample struct {
	Work       float64
	Duration   time.Duration
	Successful bool
}

// Prediction describes the expected wall time for an operation.
type Prediction struct {
	Duration   time.Duration
	Samples    int
	InFlight   int
	Calibrated bool
}

// RollingPredictor learns operation duration from a bounded, expiring sample
// window. Models are isolated by key and the least recently used key is evicted
// when MaxKeys is reached.
type RollingPredictor struct {
	config PredictorConfig
	now    func() time.Time
	mu     sync.Mutex
	models map[string]*predictionModel
}

type predictionModel struct {
	samples  []timedSample
	inFlight int
	lastUsed time.Time
}

type timedSample struct {
	at          time.Time
	work        float64
	duration    float64
	concurrency int
}

// Measurement represents an admitted operation. Finish must be called exactly
// once so the predictor's in-flight count remains accurate.
type Measurement struct {
	predictor *RollingPredictor
	model     *predictionModel
	active    int
	once      sync.Once
}

func NewRollingPredictor(config PredictorConfig) *RollingPredictor {
	if config.Window <= 0 {
		config.Window = 5 * time.Minute
	}
	if config.Capacity <= 0 {
		config.Capacity = 128
	}
	if config.MaxKeys <= 0 {
		config.MaxKeys = 5
	}
	if config.MinSamples <= 0 {
		config.MinSamples = 10
	}
	return &RollingPredictor{config: config, now: time.Now, models: make(map[string]*predictionModel)}
}

// Begin records an operation as in flight. Starting before queue acquisition
// lets callers see saturation even while work is waiting for a worker.
func (predictor *RollingPredictor) Begin(key string) *Measurement {
	if predictor == nil {
		return &Measurement{}
	}
	predictor.mu.Lock()
	now := predictor.now()
	model := predictor.modelLocked(key, now)
	model.inFlight++
	active := model.inFlight
	predictor.mu.Unlock()
	return &Measurement{predictor: predictor, model: model, active: active}
}

// Finish removes the operation from the in-flight count and, on success,
// records its processing duration. Failed and cancelled work is excluded
// because it does not represent the cost of completing an operation.
func (measurement *Measurement) Finish(sample OperationSample) {
	if measurement == nil || measurement.predictor == nil {
		return
	}
	measurement.once.Do(func() {
		predictor := measurement.predictor
		predictor.mu.Lock()
		defer predictor.mu.Unlock()
		now := predictor.now()
		model := measurement.model
		if model.inFlight > 0 {
			model.inFlight--
		}
		if !sample.Successful || sample.Work <= 0 || sample.Duration <= 0 {
			return
		}
		predictor.pruneLocked(model, now)
		model.samples = append(model.samples, timedSample{at: now, work: sample.Work, duration: sample.Duration.Seconds(), concurrency: measurement.active})
		if excess := len(model.samples) - predictor.config.Capacity; excess > 0 {
			copy(model.samples, model.samples[excess:])
			model.samples = model.samples[:predictor.config.Capacity]
		}
	})
}

// Predict estimates duration from recent successful samples. Calibrated is
// false until MinSamples remain in the time window; callers should not reject
// work based on an uncalibrated prediction.
func (predictor *RollingPredictor) Predict(key string, work float64) Prediction {
	if predictor == nil || work <= 0 {
		return Prediction{}
	}
	predictor.mu.Lock()
	defer predictor.mu.Unlock()
	now := predictor.now()
	model := predictor.modelLocked(key, now)
	predictor.pruneLocked(model, now)
	result := Prediction{Samples: len(model.samples), InFlight: model.inFlight}
	if len(model.samples) < predictor.config.MinSamples {
		return result
	}
	intercept, slope := robustLine(model.samples)
	seconds := math.Max(0, intercept+slope*work)
	seconds += upperResidual(model.samples, intercept, slope)

	// Completed samples were observed under some concurrency. Scaling only when
	// current demand exceeds that typical level avoids pretending a saturated
	// worker will retain its idle-machine throughput.
	typicalConcurrency := medianConcurrency(model.samples)
	requestedConcurrency := model.inFlight + 1
	if requestedConcurrency > typicalConcurrency && typicalConcurrency > 0 {
		seconds *= float64(requestedConcurrency) / float64(typicalConcurrency)
	}
	result.Duration = time.Duration(seconds * float64(time.Second))
	result.Calibrated = true
	return result
}

func (predictor *RollingPredictor) modelLocked(key string, now time.Time) *predictionModel {
	if model := predictor.models[key]; model != nil {
		model.lastUsed = now
		return model
	}
	if len(predictor.models) >= predictor.config.MaxKeys {
		var oldestKey string
		var oldest time.Time
		for candidateKey, model := range predictor.models {
			if oldest.IsZero() || model.lastUsed.Before(oldest) {
				oldestKey, oldest = candidateKey, model.lastUsed
			}
		}
		// Measurements retain their model pointer after eviction, so an in-flight
		// operation can finish safely without allowing rare keys to defeat the cap.
		delete(predictor.models, oldestKey)
	}
	model := &predictionModel{lastUsed: now}
	predictor.models[key] = model
	return model
}

func (predictor *RollingPredictor) pruneLocked(model *predictionModel, now time.Time) {
	cutoff := now.Add(-predictor.config.Window)
	first := 0
	for first < len(model.samples) && model.samples[first].at.Before(cutoff) {
		first++
	}
	if first > 0 {
		model.samples = append(model.samples[:0], model.samples[first:]...)
	}
}

// robustLine uses a Theil-Sen fit (median pairwise slope and median intercept).
// It handles fixed startup overhead and is much less sensitive than an average
// to a single stalled reader, writer, or scheduler pause.
func robustLine(samples []timedSample) (float64, float64) {
	slopes := make([]float64, 0, len(samples)*len(samples)/2)
	for left := 0; left < len(samples); left++ {
		for right := left + 1; right < len(samples); right++ {
			deltaWork := samples[right].work - samples[left].work
			if math.Abs(deltaWork) > 1e-9 {
				slopes = append(slopes, (samples[right].duration-samples[left].duration)/deltaWork)
			}
		}
	}
	slope := 0.0
	if len(slopes) > 0 {
		slope = math.Max(0, median(slopes))
	} else {
		ratios := make([]float64, len(samples))
		for index, sample := range samples {
			ratios[index] = sample.duration / sample.work
		}
		slope = math.Max(0, median(ratios))
	}
	intercepts := make([]float64, len(samples))
	for index, sample := range samples {
		intercepts[index] = sample.duration - slope*sample.work
	}
	return math.Max(0, median(intercepts)), slope
}

func median(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	return sorted[middle]
}

func medianConcurrency(samples []timedSample) int {
	values := make([]int, len(samples))
	for index, sample := range samples {
		values[index] = sample.concurrency
	}
	sort.Ints(values)
	return values[len(values)/2]
}

// upperResidual adds the recent 75th-percentile underprediction to the fitted
// line. This makes admission estimates mildly conservative without allowing a
// single pathological sample to dominate them as a maximum would.
func upperResidual(samples []timedSample, intercept, slope float64) float64 {
	residuals := make([]float64, len(samples))
	for index, sample := range samples {
		residuals[index] = math.Max(0, sample.duration-(intercept+slope*sample.work))
	}
	sort.Float64s(residuals)
	index := int(math.Ceil(0.75*float64(len(residuals)))) - 1
	return residuals[index]
}
