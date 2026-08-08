package ffmpeg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os/exec"
	"testing"
	"time"
)

func tinyWAV() []byte {
	const samples = 160
	b := new(bytes.Buffer)
	b.WriteString("RIFF")
	_ = binary.Write(b, binary.LittleEndian, uint32(36+samples*2))
	b.WriteString("WAVEfmt ")
	_ = binary.Write(b, binary.LittleEndian, uint32(16))
	_ = binary.Write(b, binary.LittleEndian, uint16(1))
	_ = binary.Write(b, binary.LittleEndian, uint16(1))
	_ = binary.Write(b, binary.LittleEndian, uint32(16000))
	_ = binary.Write(b, binary.LittleEndian, uint32(32000))
	_ = binary.Write(b, binary.LittleEndian, uint16(2))
	_ = binary.Write(b, binary.LittleEndian, uint16(16))
	b.WriteString("data")
	_ = binary.Write(b, binary.LittleEndian, uint32(samples*2))
	b.Write(make([]byte, samples*2))
	return b.Bytes()
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
	p, err := NewProcessor(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	wav := tinyWAV()
	info, original, err := p.Probe(context.Background(), bytes.NewReader(wav))
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
		t.Fatalf("WAV reported as needing seekable input: %q", info.Format.Name)
	}
}

func TestConvert(t *testing.T) {
	requireFFmpeg(t)
	p, err := NewProcessor(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var out bytes.Buffer
	if err := p.Convert(context.Background(), bytes.NewReader(tinyWAV()), &out); err != nil {
		t.Fatal(err)
	}
	if want := 160 * 2; out.Len() != want {
		t.Fatalf("got %d PCM bytes, want %d", out.Len(), want)
	}
}

func TestPolicies(t *testing.T) {
	for _, format := range []string{"wav", "mp3", "flac", "ogg", "opus", "aac", "adts", "webm", "matroska,webm"} {
		if NeedsSeekableInput(Info{Format: FormatInfo{Name: format}}) {
			t.Errorf("%s should stream", format)
		}
	}
	for _, format := range []string{"mov,mp4,m4a,3gp,3g2,mj2", "mp4", "m4b", "unknown", ""} {
		if !NeedsSeekableInput(Info{Format: FormatInfo{Name: format}}) {
			t.Errorf("%s should require seekability", format)
		}
	}
	target := Info{Format: FormatInfo{Name: "s16le"}, Streams: []StreamInfo{{CodecType: "audio", CodecName: "pcm_s16le", Channels: 1, SampleRate: "16000"}}}
	if !IsTargetFormat(target) {
		t.Error("target format not recognized")
	}
	target.Streams[0].Channels = 2
	if IsTargetFormat(target) {
		t.Error("stereo recognized as target")
	}
}

func TestAcquireCancellation(t *testing.T) {
	requireFFmpeg(t)
	p, err := NewProcessor(context.Background(), Config{FFprobePoolSize: 1, FFmpegPoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	first, err := p.probe.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Occupy the replacement too, leaving no ready child.
	var second *child
	deadline := time.Now().Add(2 * time.Second)
	for second == nil && time.Now().Before(deadline) {
		select {
		case second = <-p.probe.ready:
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if second == nil {
		t.Fatal("replacement was not started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.probe.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	for _, c := range []*child{first, second} {
		_ = c.stdin.Close()
		_ = c.cmd.Process.Kill()
		_ = c.cmd.Wait()
		p.probe.finished(c)
	}
}
