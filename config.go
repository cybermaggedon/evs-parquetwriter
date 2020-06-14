package main

import (
	"time"
)

type WriterConfig struct {
	flush_size uint64
	flush_interval  time.Duration
	directory  string
	output_threads int64
}

func NewWriter() *WriterConfig {
	return &WriterConfig{
		directory: ".",
		flush_size: 10 * 1024 * 1024,
		flush_interval: time.Duration(5 * time.Minute),
		output_threads: 4,
	}
}

func (wc WriterConfig) FlushSize(val uint64) *WriterConfig {
	wc.flush_size = val
	return &wc
}

func (wc WriterConfig) FlushInterval(val time.Duration) *WriterConfig {
	wc.flush_interval = val
	return &wc
}

func (wc WriterConfig) Directory(val string) *WriterConfig {
	wc.directory = val
	return &wc
}

func (wc WriterConfig) OutputThreads(val int64) *WriterConfig {
	wc.output_threads = val
	return &wc
}
