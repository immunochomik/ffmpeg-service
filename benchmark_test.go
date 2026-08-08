package ffmpeg

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"testing"
)

func coldProbe(data []byte) error {
	command := exec.Command("ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", "pipe:0")
	command.Stdin = bytes.NewReader(data)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	return command.Run()
}

func coldConvert(data []byte) error {
	command := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-i", "pipe:0", "-ac", "1", "-ar", "16000", "-acodec", "pcm_s16le", "-f", "s16le", "pipe:1")
	command.Stdin = bytes.NewReader(data)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	return command.Run()
}

func BenchmarkFFprobe(b *testing.B) {
	requireFFmpeg(b)
	data := tinyWAV()
	b.Run("cold", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			if err := coldProbe(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("warm_pool", func(b *testing.B) {
		processor, err := NewProcessor(context.Background(), Config{})
		if err != nil {
			b.Fatal(err)
		}
		defer processor.Close()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if _, _, err := processor.Probe(context.Background(), bytes.NewReader(data)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFFmpeg(b *testing.B) {
	requireFFmpeg(b)
	data := tinyWAV()
	b.Run("cold", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			if err := coldConvert(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("warm_pool", func(b *testing.B) {
		processor, err := NewProcessor(context.Background(), Config{})
		if err != nil {
			b.Fatal(err)
		}
		defer processor.Close()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			if _, err := processor.Convert(context.Background(), bytes.NewReader(data), io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkProbeAndConvert(b *testing.B) {
	requireFFmpeg(b)
	data := tinyWAV()
	b.Run("cold", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			if err := coldProbe(data); err != nil {
				b.Fatal(err)
			}
			if err := coldConvert(data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("warm_pool", func(b *testing.B) {
		processor, err := NewProcessor(context.Background(), Config{})
		if err != nil {
			b.Fatal(err)
		}
		defer processor.Close()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			_, original, err := processor.Probe(context.Background(), bytes.NewReader(data))
			if err != nil {
				b.Fatal(err)
			}
			if _, err = processor.Convert(context.Background(), original, io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
}
