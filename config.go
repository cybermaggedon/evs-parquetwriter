package main

import (
	"github.com/c2h5oh/datasize"
	"github.com/cybermaggedon/evs-golang-api"
	"log"
	"os"
	"time"
)

type PWConfig struct {
	*evs.Config
	flush_count    uint64
	flush_interval time.Duration
	directory      string
	output_threads int64
}

func NewPWConfig() *PWConfig {

	base := evs.NewConfig("evs-parquetwriter", "withioc", nil)

	c := &PWConfig{
		Config:         base,
		directory:      ".",
		flush_count:    1 * 1000 * 1000, // 1M
		flush_interval: time.Duration(5 * time.Minute),
		output_threads: 4,
	}

	if val, ok := os.LookupEnv("PARQUET_FLUSH_COUNT"); ok {
		var v datasize.ByteSize
		_ = v.UnmarshalText([]byte(val))
		log.Printf("Flush count is %d", v.Bytes())
		c.FlushCount(v.Bytes())
	}

	if val, ok := os.LookupEnv("PARQUET_FLUSH_INTERVAL"); ok {
		dur, _ := time.ParseDuration(val)
		log.Printf("Flush interval is %d seconds", int(dur.Seconds()))
		c.FlushInterval(dur)
	}

	if val, ok := os.LookupEnv("PARQUET_DIRECTORY"); ok {
		c.Directory(val)
	}

	return c

}

func (pwc *PWConfig) FlushCount(val uint64) {
	pwc.flush_count = val
}

func (pwc *PWConfig) FlushInterval(val time.Duration) {
	pwc.flush_interval = val
}

func (pwc *PWConfig) Directory(val string) {
	pwc.directory = val
}

func (pwc *PWConfig) OutputThreads(val int64) {
	pwc.output_threads = val
}
