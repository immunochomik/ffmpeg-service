package ffmpeg

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"testing"
)

func coldProbe(data []byte) error {
	c := exec.Command("ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", "pipe:0")
	c.Stdin = bytes.NewReader(data)
	c.Stdout = io.Discard
	c.Stderr = io.Discard
	return c.Run()
}

func coldConvert(data []byte) error {
	c := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le", "-f", "s16le", "pipe:1")
	c.Stdin = bytes.NewReader(data)
	c.Stdout = io.Discard
	c.Stderr = io.Discard
	return c.Run()
}

func BenchmarkFFprobe(b *testing.B) {
	requireFFmpeg(b)
	data := tinyWAV()
	b.Run("cold", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := coldProbe(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("warm_pool", func(b *testing.B) {
		p, err := NewProcessor(context.Background(), Config{})
		if err != nil {
			b.Fatal(err)
		}
		defer p.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := p.Probe(context.Background(), bytes.NewReader(data)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFFmpeg(b *testing.B) {
	requireFFmpeg(b)
	data := tinyWAV()
	b.Run("cold", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := coldConvert(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("warm_pool", func(b *testing.B) {
		p, err := NewProcessor(context.Background(), Config{})
		if err != nil {
			b.Fatal(err)
		}
		defer p.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := p.Convert(context.Background(), bytes.NewReader(data), io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkProbeAndConvert(b *testing.B) {
	requireFFmpeg(b)
	data := tinyWAV()
	b.Run("cold", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := coldProbe(data); err != nil {
				b.Fatal(err)
			}
			if err := coldConvert(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("warm_pool", func(b *testing.B) {
		p, err := NewProcessor(context.Background(), Config{})
		if err != nil {
			b.Fatal(err)
		}
		defer p.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, original, err := p.Probe(context.Background(), bytes.NewReader(data))
			if err != nil {
				b.Fatal(err)
			}
			if err = p.Convert(context.Background(), original, io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
}
