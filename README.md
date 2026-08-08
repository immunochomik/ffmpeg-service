# ffmpeg-service

`ffmpeg-service` is a small Go package that maintains pools of prestarted,
one-shot `ffprobe` and `ffmpeg` processes. Each child handles exactly one job,
exits, and is replaced. This moves much of the process startup cost out of the
request path while retaining process isolation between jobs.

The package uses `os/exec`; it does not require libav or cgo.

## Requirements

- Go 1.23 or newer
- `ffmpeg` and `ffprobe` available on `PATH`, or explicit command paths in the
  configuration

## Installation

```sh
go get github.com/immunochomik/ffmpeg-service@latest
```

## Usage

```go
package main

import (
	"bytes"
	"context"
	"io"
	"os"

	ffmpeg "github.com/immunochomik/ffmpeg-service"
)

func process(ctx context.Context, input io.Reader) error {
	processor, err := ffmpeg.NewProcessor(ctx, ffmpeg.Config{
		FFprobePoolSize: 2,
		FFmpegPoolSize:  4,
	})
	if err != nil {
		return err
	}
	defer processor.Close()

	info, original, err := processor.Probe(ctx, input)
	if err != nil {
		return err
	}

	if ffmpeg.NeedsSeekableInput(info) {
		// Spool original to a seekable source and use a different conversion
		// path. Convert accepts a non-seekable pipe input.
		return nil
	}

	if processor.IsTargetFormat(info) {
		_, err = io.Copy(os.Stdout, original)
		return err
	}

	var pcm bytes.Buffer
	converted, err := processor.Convert(ctx, original, &pcm)
	if err != nil {
		return err
	}

	// pcm contains mono, 16 kHz, signed 16-bit little-endian raw PCM.
	_ = converted // Metadata describing pcm.
	return nil
}
```

`Probe` returns metadata and a reader for the complete original input. Bytes
consumed while probing are buffered and replayed before unread bytes from the
provided reader. Callers should consume the returned reader instead of
continuing to read directly from the original reader.

Both `Probe` and `Convert` return normalized metadata:

```go
type ProbeResult struct {
	Duration          float64 `json:"duration"`
	NumChannels       int     `json:"num_channels"`
	StreamIDs         []int   `json:"stream_ids"`
	FormatName        string  `json:"format_name"`
	SampleRate        int     `json:"sample_rate"`
	DurationEstimated bool    `json:"duration_estimated"`
}
```

`Probe` prefers duration metadata reported by ffprobe. `Convert` calculates an
exact duration from the number of emitted PCM samples for recognizable raw PCM
and PCM WAV targets. For target formats whose duration cannot be derived from
the output, duration is zero.

## Container policy

`NeedsSeekableInput` is deliberately conservative:

- WAV, MP3, FLAC, Ogg/Opus, AAC/ADTS, WebM, and Matroska are treated as
  streamable.
- MOV/MP4-family containers such as M4A, MP4, MOV, 3GP, and M4B require a
  seekable input.
- Unknown formats require a seekable input.

The helper reports policy only; it does not spool input automatically.

## Configuration

```go
type Config struct {
	FFmpegCommand   string // default: ffmpeg
	FFprobeCommand  string // default: ffprobe
	FFmpegPoolSize  int    // default: 1
	FFprobePoolSize int    // default: 1
	FFmpegArgs      []string
	FFprobeArgs     []string
}
```

The default conversion is equivalent to:

```text
ffmpeg -hide_banner -loglevel error -i pipe:0 -ac 1 -ar 16000 \
  -acodec pcm_s16le -f s16le pipe:1
```

Acquisition waits when a pool is exhausted and respects context cancellation.
Closing the processor terminates idle children and waits for them to be reaped.

## Benchmarks

Run the cold-process versus warm-pool benchmarks with:

```sh
go test -run '^$' \
  -bench 'Benchmark(FFprobe|FFmpeg|ProbeAndConvert)$' \
  -benchmem -count=5
```

Benchmark results depend heavily on FFmpeg version, input size, hardware, and
system load.

## License

See [LICENSE](LICENSE).
