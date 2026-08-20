package ffmpeg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"testing"
	"time"
)

func tinyWAV() []byte {
	const samples = 160
	buffer := new(bytes.Buffer)
	buffer.WriteString("RIFF")
	_ = binary.Write(buffer, binary.LittleEndian, uint32(36+samples*2))
	buffer.WriteString("WAVEfmt ")
	_ = binary.Write(buffer, binary.LittleEndian, uint32(16))
	_ = binary.Write(buffer, binary.LittleEndian, uint16(1))
	_ = binary.Write(buffer, binary.LittleEndian, uint16(1))
	_ = binary.Write(buffer, binary.LittleEndian, uint32(16000))
	_ = binary.Write(buffer, binary.LittleEndian, uint32(32000))
	_ = binary.Write(buffer, binary.LittleEndian, uint16(2))
	_ = binary.Write(buffer, binary.LittleEndian, uint16(16))
	buffer.WriteString("data")
	_ = binary.Write(buffer, binary.LittleEndian, uint32(samples*2))
	buffer.Write(make([]byte, samples*2))
	return buffer.Bytes()
}

func requireFFmpeg(t testing.TB) {
	t.Helper()
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skipf("%s not installed", name)
		}
	}
}

func TestProbePreservesInput(t *testing.T) {
	requireFFmpeg(t)
	processor, err := NewProcessor(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	wav := tinyWAV()
	info, original, err := processor.Probe(context.Background(), bytes.NewReader(wav))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(original)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, wav) {
		t.Fatalf("original input changed: got %d bytes, want %d", len(got), len(wav))
	}
	if NeedsSeekableInput(info) {
		t.Fatalf("WAV reported as needing seekable input: %q", info.FormatName)
	}
	if info.NumChannels != 1 || info.SampleRate != 16000 || len(info.StreamIDs) != 1 {
		t.Fatalf("unexpected probe result: %+v", info)
	}
}

func TestConvert(t *testing.T) {
	requireFFmpeg(t)
	processor, err := NewProcessor(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	var out bytes.Buffer
	result, err := processor.Convert(context.Background(), bytes.NewReader(tinyWAV()), &out)
	if err != nil {
		t.Fatal(err)
	}
	if want := 160 * 2; out.Len() != want {
		t.Fatalf("got %d PCM bytes, want %d", out.Len(), want)
	}
	if result.FormatName != "s16le" || result.NumChannels != 1 || result.SampleRate != 16000 || result.Duration != 0.01 || result.DurationEstimated {
		t.Fatalf("unexpected conversion result: %+v", result)
	}
}

func TestDefaultArgsRestrictInputProtocols(t *testing.T) {
	for name, test := range map[string]struct {
		args []string
		want []string
	}{
		"ffmpeg":  {defaultFFmpegArgs(), []string{"-protocol_whitelist", "pipe", "-i", "pipe:0"}},
		"ffprobe": {defaultFFprobeArgs(), []string{"-protocol_whitelist", "pipe", "-show_format"}},
	} {
		found := false
		for index := 0; index+len(test.want) <= len(test.args); index++ {
			if reflect.DeepEqual(test.args[index:index+len(test.want)], test.want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s args %q do not contain input restriction %q", name, test.args, test.want)
		}
	}
}

func TestConvertWAVDuration(t *testing.T) {
	requireFFmpeg(t)
	processor, err := NewProcessor(context.Background(), Config{FFmpegArgs: []string{
		"-hide_banner", "-loglevel", "error", "-i", "pipe:0",
		"-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le", "-f", "wav", "pipe:1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()

	var output bytes.Buffer
	result, err := processor.Convert(context.Background(), bytes.NewReader(tinyWAV()), &output)
	if err != nil {
		t.Fatal(err)
	}
	if result.FormatName != "wav" || result.Duration != 0.01 || result.DurationEstimated {
		t.Fatalf("unexpected WAV conversion result: %+v", result)
	}
}

func TestConvertProbedFeedsPredictorAndMetrics(t *testing.T) {
	requireFFmpeg(t)
	predictor := NewRollingPredictor(PredictorConfig{MinSamples: 1})
	var metrics ConvertMetrics
	processor, err := NewProcessor(context.Background(), Config{
		Predictor: predictor,
		OnConvert: func(observed ConvertMetrics) { metrics = observed },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	inputInfo := ProbeResult{Duration: 0.01, FormatName: "wav", codecName: "pcm_s16le", NumChannels: 1, SampleRate: 16000}
	var output bytes.Buffer
	if _, err := processor.ConvertProbed(context.Background(), bytes.NewReader(tinyWAV()), &output, inputInfo); err != nil {
		t.Fatal(err)
	}
	prediction, calibrated := processor.PredictConvert(inputInfo)
	if !calibrated || prediction.Duration <= 0 || prediction.Samples != 1 {
		t.Fatalf("unexpected prediction: %+v", prediction)
	}
	if !metrics.Successful || metrics.AudioType != inputInfo.AudioType() || metrics.AudioDuration != 0.01 || metrics.ProcessingDuration <= 0 || metrics.TotalDuration < metrics.ProcessingDuration {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
}

func TestPolicies(t *testing.T) {
	for _, format := range []string{"wav", "mp3", "flac", "ogg", "opus", "aac", "adts", "webm", "matroska,webm"} {
		if NeedsSeekableInput(ProbeResult{FormatName: format}) {
			t.Errorf("%s should stream", format)
		}
	}
	for _, format := range []string{"mov,mp4,m4a,3gp,3g2,mj2", "mp4", "m4b", "unknown", ""} {
		if !NeedsSeekableInput(ProbeResult{FormatName: format}) {
			t.Errorf("%s should require seekability", format)
		}
	}
	processor := &Processor{target: parseOutputFormat([]string{"-i", "pipe:0", "-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le", "-f", "s16le", "pipe:1"})}
	target := ProbeResult{FormatName: "s16le", codecName: "pcm_s16le", NumChannels: 1, SampleRate: 16000}
	if !processor.IsTargetFormat(target) {
		t.Error("target format not recognized")
	}
	target.NumChannels = 2
	if processor.IsTargetFormat(target) {
		t.Error("stereo recognized as target")
	}
}

func TestIsTargetFormatUsesConfiguredArgs(t *testing.T) {
	processor := &Processor{target: parseOutputFormat([]string{
		"-f", "rawvideo", "-i", "pipe:0", // input option: must be ignored
		"-ac", "2", "-ar", "48000", "-c:a", "libopus", "-f", "ogg", "pipe:1",
	})}
	info := ProbeResult{FormatName: "ogg", codecName: "opus", NumChannels: 2, SampleRate: 48000}
	if !processor.IsTargetFormat(info) {
		t.Fatal("configured Ogg/Opus target not recognized")
	}
	info = ProbeResult{FormatName: "s16le", codecName: "pcm_s16le", NumChannels: 1, SampleRate: 16000}
	if processor.IsTargetFormat(info) {
		t.Fatal("hard-coded default target was recognized for custom args")
	}
}

func TestRawPCMDuration(t *testing.T) {
	if duration := pcmDuration(32000, nil, "s16le", "pcm_s16le", 1, 16000); duration != 1 {
		t.Fatalf("got duration %v, want 1", duration)
	}
	wav := tinyWAV()
	if duration := pcmDuration(int64(len(wav)), wav, "wav", "pcm_s16le", 1, 16000); duration != 0.01 {
		t.Fatalf("got WAV duration %v, want 0.01", duration)
	}
}

func TestAcquireCancellation(t *testing.T) {
	requireFFmpeg(t)
	processor, err := NewProcessor(context.Background(), Config{FFprobePoolSize: 1, FFmpegPoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	first, err := processor.probe.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Occupy the replacement too, leaving no ready child.
	var second *child
	deadline := time.Now().Add(2 * time.Second)
	for second == nil && time.Now().Before(deadline) {
		select {
		case second = <-processor.probe.ready:
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if second == nil {
		t.Fatal("replacement was not started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := processor.probe.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	for _, process := range []*child{first, second} {
		_ = process.stdin.Close()
		_ = process.cmd.Process.Kill()
		_ = process.cmd.Wait()
		processor.probe.finished(process)
	}
}
