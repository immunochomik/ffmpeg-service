# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-08-08

Initial release.

- Add configurable pools of prestarted, one-shot `ffprobe` and `ffmpeg`
  processes.
- Preserve the complete original input after probing.
- Expose normalized audio metadata and conservative seekability policy.
- Detect whether input already matches the configured conversion target.
- Calculate exact output duration for supported raw PCM and PCM WAV targets.
- Add rolling conversion-load prediction with per-audio-type LRU models,
  robust estimates, calibration state, and detailed timing metrics.
- Add cold-start versus warm-pool benchmarks.

[0.1.0]: https://github.com/immunochomik/ffmpeg-service/releases/tag/v0.1.0
