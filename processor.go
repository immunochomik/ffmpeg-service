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

type Info struct {
	Format  FormatInfo   `json:"format"`
	Streams []StreamInfo `json:"streams"`
}

type FormatInfo struct {
	Name     string            `json:"format_name"`
	LongName string            `json:"format_long_name"`
	Duration string            `json:"duration"`
	BitRate  string            `json:"bit_rate"`
	Tags     map[string]string `json:"tags"`
}

func (f FormatInfo) DurationSeconds() (float64, error) {
	return strconv.ParseFloat(f.Duration, 64)
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
		cfg.FFmpegPoolSize = 1
	}
	if cfg.FFprobePoolSize == 0 {
		cfg.FFprobePoolSize = 1
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
	return &Processor{probe: probe, convert: convert}, nil
}

func (p *Processor) Probe(ctx context.Context, input io.Reader) (Info, io.Reader, error) {
	if input == nil {
		return Info{}, nil, errors.New("ffmpeg: nil input")
	}
	proc, err := p.probe.acquire(ctx)
	if err != nil {
		return Info{}, input, err
	}
	stopCancel := proc.watch(ctx)
	var saved bytes.Buffer
	copyErr := make(chan error, 1)
	go func() {
		_, e := io.Copy(proc.stdin, io.TeeReader(input, &saved))
		if ce := proc.stdin.Close(); e == nil {
			e = ce
		}
		copyErr <- e
	}()
	var info Info
	decodeErr := json.NewDecoder(proc.stdout).Decode(&info)
	_, _ = io.Copy(io.Discard, proc.stdout)
	waitErr := proc.wait()
	stopCancel()
	p.probe.finished(proc)
	writeErr := <-copyErr
	original := io.MultiReader(bytes.NewReader(saved.Bytes()), input)
	if ctx.Err() != nil {
		return Info{}, original, ctx.Err()
	}
	if decodeErr != nil {
		return Info{}, original, fmt.Errorf("decode ffprobe output: %w%s", decodeErr, proc.stderrSuffix())
	}
	if waitErr != nil {
		return Info{}, original, fmt.Errorf("ffprobe: %w%s", waitErr, proc.stderrSuffix())
	}
	if writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
		return Info{}, original, fmt.Errorf("write ffprobe input: %w", writeErr)
	}
	return info, original, nil
}

func (p *Processor) Convert(ctx context.Context, input io.Reader, output io.Writer) error {
	if input == nil || output == nil {
		return errors.New("ffmpeg: nil input or output")
	}
	proc, err := p.convert.acquire(ctx)
	if err != nil {
		return err
	}
	stopCancel := proc.watch(ctx)
	copyErr := make(chan error, 1)
	go func() {
		_, e := io.Copy(proc.stdin, input)
		if ce := proc.stdin.Close(); e == nil {
			e = ce
		}
		copyErr <- e
	}()
	_, outputErr := io.Copy(output, proc.stdout)
	// A destination error must not leave the child blocked on a full stdout
	// pipe while Wait waits for that child to exit.
	if outputErr != nil {
		_, _ = io.Copy(io.Discard, proc.stdout)
	}
	waitErr := proc.wait()
	stopCancel()
	p.convert.finished(proc)
	writeErr := <-copyErr
	if outputErr != nil {
		return fmt.Errorf("read ffmpeg output: %w", outputErr)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if waitErr != nil {
		return fmt.Errorf("ffmpeg: %w%s", waitErr, proc.stderrSuffix())
	}
	if writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
		return fmt.Errorf("write ffmpeg input: %w", writeErr)
	}
	return nil
}

func (p *Processor) Close() error {
	p.closeOnce.Do(func() { p.probe.close(); p.convert.close() })
	return nil
}

// NeedsSeekableInput is deliberately conservative. Unknown formats and the
// MOV/MP4 family require a seekable source; known sequential formats do not.
func NeedsSeekableInput(info Info) bool {
	streamable := map[string]bool{"wav": true, "mp3": true, "flac": true, "ogg": true, "opus": true, "aac": true, "adts": true, "webm": true, "matroska": true}
	for _, name := range strings.Split(strings.ToLower(info.Format.Name), ",") {
		if streamable[strings.TrimSpace(name)] {
			return false
		}
	}
	return true
}

// IsTargetFormat reports whether the first audio stream is mono, 16 kHz,
// signed 16-bit little-endian PCM in a raw s16le container.
func IsTargetFormat(info Info) bool {
	formatOK := false
	for _, n := range strings.Split(strings.ToLower(info.Format.Name), ",") {
		if strings.TrimSpace(n) == "s16le" {
			formatOK = true
		}
	}
	if !formatOK {
		return false
	}
	for _, s := range info.Streams {
		if s.CodecType == "audio" {
			return s.CodecName == "pcm_s16le" && s.Channels == 1 && s.SampleRate == "16000"
		}
	}
	return false
}

type child struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr bytes.Buffer
}

func (c *child) watch(ctx context.Context) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = c.cmd.Process.Kill()
		case <-done:
		}
	}()
	return func() { close(done) }
}

func startChild(command string, args []string) (*child, error) {
	c := &child{}
	c.cmd = exec.Command(command, args...)
	c.cmd.Stderr = &c.stderr
	var err error
	if c.stdin, err = c.cmd.StdinPipe(); err != nil {
		return nil, err
	}
	if c.stdout, err = c.cmd.StdoutPipe(); err != nil {
		return nil, err
	}
	if err = c.cmd.Start(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *child) wait() error { return c.cmd.Wait() }
func (c *child) stderrSuffix() string {
	s := strings.TrimSpace(c.stderr.String())
	if s == "" {
		return ""
	}
	return ": " + s
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
	p := &processPool{ctx: poolCtx, cancel: cancel, ready: make(chan *child, size), command: command, args: append([]string(nil), args...), children: make(map[*child]struct{})}
	for i := 0; i < size; i++ {
		c, err := startChild(command, args)
		if err != nil {
			p.close()
			return nil, err
		}
		p.track(c)
		p.ready <- c
	}
	return p, nil
}

func (p *processPool) acquire(ctx context.Context) (*child, error) {
	if ctx == nil {
		return nil, errors.New("ffmpeg: nil context")
	}
	select {
	case <-p.ctx.Done():
		return nil, ErrClosed
	default:
	}
	select {
	case c := <-p.ready:
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = c.stdin.Close()
			_ = c.cmd.Process.Kill()
			_ = c.cmd.Wait()
			p.finished(c)
			return nil, ErrClosed
		}
		p.wg.Add(1)
		p.mu.Unlock()
		go p.replace()
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, ErrClosed
	}
}

func (p *processPool) replace() {
	defer p.wg.Done()
	c, err := startChild(p.command, p.args)
	if err != nil {
		return
	}
	p.track(c)
	select {
	case p.ready <- c:
	case <-p.ctx.Done():
		_ = c.stdin.Close()
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
		p.finished(c)
	}
}

func (p *processPool) track(c *child) {
	p.mu.Lock()
	p.children[c] = struct{}{}
	p.childrenWG.Add(1)
	p.mu.Unlock()
}

func (p *processPool) finished(c *child) {
	p.mu.Lock()
	if _, ok := p.children[c]; ok {
		delete(p.children, c)
		p.childrenWG.Done()
	}
	p.mu.Unlock()
}

func (p *processPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cancel()
	p.mu.Unlock()
	p.wg.Wait()
	for {
		select {
		case c := <-p.ready:
			_ = c.stdin.Close()
			_ = c.cmd.Process.Kill()
			_ = c.cmd.Wait()
			p.finished(c)
		default:
			p.mu.Lock()
			for c := range p.children {
				_ = c.cmd.Process.Kill()
			}
			p.mu.Unlock()
			p.childrenWG.Wait()
			return
		}
	}
}
