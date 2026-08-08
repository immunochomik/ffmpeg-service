// Package ffmpeg provides pools of prestarted, one-shot ffprobe and ffmpeg
// processes. A child is never shared by two jobs or reused for a second job.
package ffmpeg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

var ErrClosed = errors.New("ffmpeg: processor closed")

type Config struct {
	FFmpegCommand   string
	FFprobeCommand  string
	FFmpegPoolSize  int
	FFprobePoolSize int

	// Args replace the defaults when non-nil. They are primarily useful for
	// wrappers and testing; an empty (but non-nil) slice is respected.
	FFmpegArgs  []string
	FFprobeArgs []string
}

type probeInfo struct {
	Format  FormatInfo   `json:"format"`
	Streams []StreamInfo `json:"streams"`
}

// ProbeResult is the normalized audio metadata returned by Probe and Convert.
type ProbeResult struct {
	Duration          float64 `json:"duration"`
	NumChannels       int     `json:"num_channels"`
	StreamIDs         []int   `json:"stream_ids"`
	FormatName        string  `json:"format_name"`
	SampleRate        int     `json:"sample_rate"`
	DurationEstimated bool    `json:"duration_estimated"`

	codecName string
}

type FormatInfo struct {
	Name     string            `json:"format_name"`
	LongName string            `json:"format_long_name"`
	Duration string            `json:"duration"`
	BitRate  string            `json:"bit_rate"`
	Tags     map[string]string `json:"tags"`
}

type StreamInfo struct {
	Index      int               `json:"index"`
	CodecName  string            `json:"codec_name"`
	CodecType  string            `json:"codec_type"`
	SampleRate string            `json:"sample_rate"`
	Channels   int               `json:"channels"`
	Duration   string            `json:"duration"`
	Tags       map[string]string `json:"tags"`
}

type Processor struct {
	probe     *processPool
	convert   *processPool
	target    outputFormat
	closeOnce sync.Once
}

func NewProcessor(ctx context.Context, cfg Config) (*Processor, error) {
	if ctx == nil {
		return nil, errors.New("ffmpeg: nil context")
	}
	if cfg.FFmpegPoolSize < 0 || cfg.FFprobePoolSize < 0 {
		return nil, errors.New("ffmpeg: pool sizes must be non-negative")
	}
	if cfg.FFmpegPoolSize == 0 {
		cfg.FFmpegPoolSize = 5
	}
	if cfg.FFprobePoolSize == 0 {
		cfg.FFprobePoolSize = 5
	}
	if cfg.FFmpegCommand == "" {
		cfg.FFmpegCommand = "ffmpeg"
	}
	if cfg.FFprobeCommand == "" {
		cfg.FFprobeCommand = "ffprobe"
	}
	if cfg.FFmpegArgs == nil {
		cfg.FFmpegArgs = []string{"-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le", "-f", "s16le", "pipe:1"}
	}
	if cfg.FFprobeArgs == nil {
		cfg.FFprobeArgs = []string{"-v", "error", "-show_format", "-show_streams", "-of", "json", "pipe:0"}
	}
	probe, err := newProcessPool(ctx, cfg.FFprobePoolSize, cfg.FFprobeCommand, cfg.FFprobeArgs)
	if err != nil {
		return nil, fmt.Errorf("start ffprobe pool: %w", err)
	}
	convert, err := newProcessPool(ctx, cfg.FFmpegPoolSize, cfg.FFmpegCommand, cfg.FFmpegArgs)
	if err != nil {
		probe.close()
		return nil, fmt.Errorf("start ffmpeg pool: %w", err)
	}
	return &Processor{probe: probe, convert: convert, target: parseOutputFormat(cfg.FFmpegArgs)}, nil
}

func (processor *Processor) Probe(ctx context.Context, input io.Reader) (ProbeResult, io.Reader, error) {
	if input == nil {
		return ProbeResult{}, nil, errors.New("ffmpeg: nil input")
	}
	process, err := processor.probe.acquire(ctx)
	if err != nil {
		return ProbeResult{}, input, err
	}
	stopCancel := process.watch(ctx)
	var saved bytes.Buffer
	copyErr := make(chan error, 1)
	go func() {
		// ffprobe consumes part or all of the non-seekable input. Tee those
		// bytes into saved so Probe can return saved + the unread remainder as
		// one reader representing the complete original stream.
		_, copyError := io.Copy(process.stdin, io.TeeReader(input, &saved))
		if closeError := process.stdin.Close(); copyError == nil {
			copyError = closeError
		}
		copyErr <- copyError
	}()
	var info probeInfo
	decodeErr := json.NewDecoder(process.stdout).Decode(&info)
	_, _ = io.Copy(io.Discard, process.stdout)
	waitErr := process.wait()
	stopCancel()
	processor.probe.finished(process)
	writeErr := <-copyErr
	original := io.MultiReader(bytes.NewReader(saved.Bytes()), input)
	if ctx.Err() != nil {
		return ProbeResult{}, original, ctx.Err()
	}
	if decodeErr != nil {
		return ProbeResult{}, original, fmt.Errorf("decode ffprobe output: %w%s", decodeErr, process.stderrSuffix())
	}
	if waitErr != nil {
		return ProbeResult{}, original, fmt.Errorf("ffprobe: %w%s", waitErr, process.stderrSuffix())
	}
	if writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
		return ProbeResult{}, original, fmt.Errorf("write ffprobe input: %w", writeErr)
	}
	return normalizeProbeInfo(info, int64(saved.Len())), original, nil
}

func (processor *Processor) Convert(ctx context.Context, input io.Reader, output io.Writer) (ProbeResult, error) {
	if input == nil || output == nil {
		return ProbeResult{}, errors.New("ffmpeg: nil input or output")
	}
	process, err := processor.convert.acquire(ctx)
	if err != nil {
		return ProbeResult{}, err
	}
	stopCancel := process.watch(ctx)
	copyErr := make(chan error, 1)
	go func() {
		_, copyError := io.Copy(process.stdin, input)
		if closeError := process.stdin.Close(); copyError == nil {
			copyError = closeError
		}
		copyErr <- copyError
	}()
	countedOutput := &countingWriter{writer: output}
	_, outputErr := io.Copy(countedOutput, process.stdout)
	// A destination error must not leave the child blocked on a full stdout
	// pipe while Wait waits for that child to exit.
	if outputErr != nil {
		_, _ = io.Copy(io.Discard, process.stdout)
	}
	waitErr := process.wait()
	stopCancel()
	processor.convert.finished(process)
	writeErr := <-copyErr
	result := processor.target.result(countedOutput.written)
	if outputErr != nil {
		return result, fmt.Errorf("read ffmpeg output: %w", outputErr)
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if waitErr != nil {
		return result, fmt.Errorf("ffmpeg: %w%s", waitErr, process.stderrSuffix())
	}
	if writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
		return result, fmt.Errorf("write ffmpeg input: %w", writeErr)
	}
	return result, nil
}

func (processor *Processor) Close() error {
	processor.closeOnce.Do(func() { processor.probe.close(); processor.convert.close() })
	return nil
}

// NeedsSeekableInput is deliberately conservative. Unknown formats and the
// MOV/MP4 family require a seekable source; known sequential formats do not.
func NeedsSeekableInput(info ProbeResult) bool {
	streamable := map[string]bool{"wav": true, "mp3": true, "flac": true, "ogg": true, "opus": true, "aac": true, "adts": true, "webm": true, "matroska": true}
	for _, name := range strings.Split(strings.ToLower(info.FormatName), ",") {
		if streamable[strings.TrimSpace(name)] {
			return false
		}
	}
	return true
}

// IsTargetFormat reports whether the first audio stream already matches the
// output format configured by the Processor's FFmpegArgs. It returns false
// when the relevant output format or codec cannot be determined from the args.
func (processor *Processor) IsTargetFormat(info ProbeResult) bool {
	if processor == nil || processor.target.container == "" || processor.target.codec == "" {
		return false
	}
	formatOK := false
	for _, formatName := range strings.Split(strings.ToLower(info.FormatName), ",") {
		if strings.TrimSpace(formatName) == processor.target.container {
			formatOK = true
		}
	}
	if !formatOK {
		return false
	}
	return strings.EqualFold(info.codecName, processor.target.codec) &&
		(processor.target.channels == 0 || info.NumChannels == processor.target.channels) &&
		(processor.target.sampleRate == 0 || info.SampleRate == processor.target.sampleRate)
}

type outputFormat struct {
	container  string
	codec      string
	channels   int
	sampleRate int
}

func parseOutputFormat(args []string) outputFormat {
	// Options before the final input apply to an input, not the output.
	start := 0
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "-i" {
			start = i + 2
			i++
		}
	}
	var target outputFormat
	for i := start; i+1 < len(args); i++ {
		value := strings.ToLower(args[i+1])
		switch args[i] {
		case "-f":
			target.container = value
			i++
		case "-acodec", "-codec:a", "-c:a":
			target.codec = canonicalCodec(value)
			i++
		case "-ac":
			target.channels, _ = strconv.Atoi(value)
			i++
		case "-ar":
			target.sampleRate, _ = strconv.Atoi(value)
			i++
		}
	}
	return target
}

func normalizeProbeInfo(info probeInfo, inputBytes int64) ProbeResult {
	result := ProbeResult{FormatName: info.Format.Name, StreamIDs: make([]int, 0)}
	result.Duration, _ = strconv.ParseFloat(info.Format.Duration, 64)
	for _, stream := range info.Streams {
		if stream.CodecType != "audio" {
			continue
		}
		result.StreamIDs = append(result.StreamIDs, stream.Index)
		if result.codecName == "" {
			result.codecName = stream.CodecName
			result.NumChannels = stream.Channels
			result.SampleRate, _ = strconv.Atoi(stream.SampleRate)
		}
		if result.Duration == 0 {
			streamDuration, _ := strconv.ParseFloat(stream.Duration, 64)
			if streamDuration > result.Duration {
				result.Duration = streamDuration
			}
		}
	}
	if result.Duration == 0 {
		result.Duration = pcmDuration(inputBytes, result.codecName, result.NumChannels, result.SampleRate)
		result.DurationEstimated = result.Duration > 0
	}
	return result
}

func (target outputFormat) result(outputBytes int64) ProbeResult {
	result := ProbeResult{
		NumChannels: target.channels,
		StreamIDs:   []int{0},
		FormatName:  target.container,
		SampleRate:  target.sampleRate,
		codecName:   target.codec,
	}
	result.Duration = pcmDuration(outputBytes, target.codec, target.channels, target.sampleRate)
	result.DurationEstimated = result.Duration > 0
	return result
}

func pcmDuration(byteCount int64, codec string, channels, sampleRate int) float64 {
	bits := 0
	if strings.HasPrefix(codec, "pcm_s") || strings.HasPrefix(codec, "pcm_u") || strings.HasPrefix(codec, "pcm_f") {
		for _, width := range []int{8, 16, 24, 32, 64} {
			if strings.Contains(codec, strconv.Itoa(width)) {
				bits = width
				break
			}
		}
	}
	if byteCount <= 0 || bits == 0 || channels <= 0 || sampleRate <= 0 {
		return 0
	}
	return float64(byteCount) / float64(bits/8*channels*sampleRate)
}

type countingWriter struct {
	writer  io.Writer
	written int64
}

func (output *countingWriter) Write(data []byte) (int, error) {
	written, err := output.writer.Write(data)
	output.written += int64(written)
	return written, err
}

func canonicalCodec(codec string) string {
	switch codec {
	case "libmp3lame":
		return "mp3"
	case "libopus":
		return "opus"
	case "libvorbis":
		return "vorbis"
	default:
		return codec
	}
}

type child struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr bytes.Buffer
}

// watch connects a job's context to a child that was started before the job
// existed and therefore could not be created with exec.CommandContext. It
// kills the child if the job is cancelled. The returned function disarms the
// watcher after normal completion so it cannot kill an already-finished child.
func (process *child) watch(ctx context.Context) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = process.cmd.Process.Kill()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func startChild(command string, args []string) (*child, error) {
	process := &child{}
	process.cmd = exec.Command(command, args...)
	process.cmd.Stderr = &process.stderr
	var err error
	if process.stdin, err = process.cmd.StdinPipe(); err != nil {
		return nil, err
	}
	if process.stdout, err = process.cmd.StdoutPipe(); err != nil {
		return nil, err
	}
	if err = process.cmd.Start(); err != nil {
		return nil, err
	}
	return process, nil
}

func (process *child) wait() error { return process.cmd.Wait() }
func (process *child) stderrSuffix() string {
	stderr := strings.TrimSpace(process.stderr.String())
	if stderr == "" {
		return ""
	}
	return ": " + stderr
}

type processPool struct {
	ctx        context.Context
	cancel     context.CancelFunc
	ready      chan *child
	command    string
	args       []string
	wg         sync.WaitGroup
	mu         sync.Mutex
	closed     bool
	children   map[*child]struct{}
	childrenWG sync.WaitGroup
}

func newProcessPool(ctx context.Context, size int, command string, args []string) (*processPool, error) {
	poolCtx, cancel := context.WithCancel(ctx)
	pool := &processPool{ctx: poolCtx, cancel: cancel, ready: make(chan *child, size), command: command, args: append([]string(nil), args...), children: make(map[*child]struct{})}
	for i := 0; i < size; i++ {
		process, err := startChild(command, args)
		if err != nil {
			pool.close()
			return nil, err
		}
		pool.track(process)
		pool.ready <- process
	}
	return pool, nil
}

func (pool *processPool) acquire(ctx context.Context) (*child, error) {
	if ctx == nil {
		return nil, errors.New("ffmpeg: nil context")
	}
	select {
	case <-pool.ctx.Done():
		return nil, ErrClosed
	default:
	}
	select {
	case process := <-pool.ready:
		pool.mu.Lock()
		// Close may begin after the initial context check but before this child
		// is received. Do not hand an idle child to a new job during shutdown;
		// remove it from tracking and reap it here instead.
		if pool.closed {
			pool.mu.Unlock()
			_ = process.stdin.Close()
			_ = process.cmd.Process.Kill()
			_ = process.cmd.Wait()
			pool.finished(process)
			return nil, ErrClosed
		}
		pool.wg.Add(1)
		pool.mu.Unlock()
		go pool.replace()
		return process, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pool.ctx.Done():
		return nil, ErrClosed
	}
}

func (pool *processPool) replace() {
	defer pool.wg.Done()
	process, err := startChild(pool.command, pool.args)
	if err != nil {
		return
	}
	pool.track(process)
	select {
	case pool.ready <- process:
	case <-pool.ctx.Done():
		_ = process.stdin.Close()
		_ = process.cmd.Process.Kill()
		_ = process.cmd.Wait()
		pool.finished(process)
	}
}

func (pool *processPool) track(process *child) {
	pool.mu.Lock()
	pool.children[process] = struct{}{}
	pool.childrenWG.Add(1)
	pool.mu.Unlock()
}

func (pool *processPool) finished(process *child) {
	pool.mu.Lock()
	if _, tracked := pool.children[process]; tracked {
		delete(pool.children, process)
		pool.childrenWG.Done()
	}
	pool.mu.Unlock()
}

func (pool *processPool) close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	pool.cancel()
	pool.mu.Unlock()
	pool.wg.Wait()
	for {
		select {
		case process := <-pool.ready:
			_ = process.stdin.Close()
			_ = process.cmd.Process.Kill()
			_ = process.cmd.Wait()
			pool.finished(process)
		default:
			pool.mu.Lock()
			for process := range pool.children {
				_ = process.cmd.Process.Kill()
			}
			pool.mu.Unlock()
			pool.childrenWG.Wait()
			return
		}
	}
}
