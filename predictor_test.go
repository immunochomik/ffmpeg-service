package ffmpeg

import (
	"testing"
	"time"
)

func TestRollingPredictorCalibrationAndOutlierResistance(t *testing.T) {
	predictor := NewRollingPredictor(PredictorConfig{MinSamples: 5, Capacity: 10})
	for _, seconds := range []float64{2, 4, 6, 8} {
		measurement := predictor.Begin("wav")
		measurement.Finish(OperationSample{Work: seconds, Duration: time.Duration(seconds/2*float64(time.Second)) + 10*time.Millisecond, Successful: true})
	}
	if prediction := predictor.Predict("wav", 10); prediction.Calibrated {
		t.Fatal("predictor calibrated before minimum sample count")
	}
	measurement := predictor.Begin("wav")
	measurement.Finish(OperationSample{Work: 10, Duration: 30 * time.Second, Successful: true}) // scheduler/I/O outlier
	prediction := predictor.Predict("wav", 10)
	if !prediction.Calibrated {
		t.Fatal("predictor did not calibrate")
	}
	if prediction.Duration < 4*time.Second || prediction.Duration > 7*time.Second {
		t.Fatalf("outlier distorted prediction: %v", prediction.Duration)
	}
}

func TestRollingPredictorExpiresSamplesAndIgnoresFailures(t *testing.T) {
	predictor := NewRollingPredictor(PredictorConfig{Window: time.Minute, MinSamples: 1})
	now := time.Unix(100, 0)
	predictor.now = func() time.Time { return now }

	failed := predictor.Begin("job")
	failed.Finish(OperationSample{Work: 1, Duration: time.Second, Successful: false})
	if prediction := predictor.Predict("job", 1); prediction.Samples != 0 {
		t.Fatalf("failed sample was learned: %+v", prediction)
	}
	successful := predictor.Begin("job")
	successful.Finish(OperationSample{Work: 1, Duration: time.Second, Successful: true})
	if prediction := predictor.Predict("job", 1); !prediction.Calibrated {
		t.Fatal("successful sample was not learned")
	}
	now = now.Add(2 * time.Minute)
	if prediction := predictor.Predict("job", 1); prediction.Calibrated || prediction.Samples != 0 {
		t.Fatalf("expired sample was retained: %+v", prediction)
	}
}

func TestRollingPredictorCapsModelsAndTracksInFlight(t *testing.T) {
	predictor := NewRollingPredictor(PredictorConfig{MaxKeys: 2, MinSamples: 1})
	now := time.Unix(100, 0)
	predictor.now = func() time.Time { return now }
	first := predictor.Begin("first")
	now = now.Add(time.Second)
	second := predictor.Begin("second")
	now = now.Add(time.Second)
	third := predictor.Begin("third")
	if len(predictor.models) != 2 {
		t.Fatalf("got %d models, want 2", len(predictor.models))
	}
	if _, retained := predictor.models["first"]; retained {
		t.Fatal("least recently used model was retained")
	}
	if prediction := predictor.Predict("third", 1); prediction.InFlight != 1 {
		t.Fatalf("got %d in flight, want 1", prediction.InFlight)
	}
	// Finishing a measurement whose active model was evicted must remain safe.
	first.Finish(OperationSample{Work: 1, Duration: time.Second, Successful: true})
	second.Finish(OperationSample{})
	third.Finish(OperationSample{})
}
