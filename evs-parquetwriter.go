//
// ElasticSearch loader.  Maps events to an ElasticSearch schema.
//

package main

import (
	evs "github.com/cybermaggedon/evs-golang-api"
	"log"
	"os"
	"github.com/c2h5oh/datasize"
	"time"
)

const ()

type ParquetWriter struct {

	// Embed EventAnalytic framework
	evs.EventAnalytic

	f Flattener

	writer *Writer
}

// Initialisation
func (p *ParquetWriter) Init(binding string) error {

	wr := NewWriter()

	if val, ok := os.LookupEnv("PARQUET_FLUSH_COUNT"); ok {
		var v datasize.ByteSize
		_ = v.UnmarshalText([]byte(val))
		log.Printf("Flush count is %d", v.Bytes())
		wr = wr.FlushCount(v.Bytes())
	}
	if val, ok := os.LookupEnv("PARQUET_FLUSH_INTERVAL"); ok {
		dur, _ := time.ParseDuration(val)
		log.Printf("Flush interval is %d seconds", int(dur.Seconds()))
		wr = wr.FlushInterval(dur)
	}
	if val, ok := os.LookupEnv("PARQUET_DIRECTORY"); ok {
		wr = wr.Directory(val)
	}

	var err error
	p.writer, err = wr.Build()
	if err != nil {
		return err
	}

	p.EventAnalytic.Init(binding, []string{}, p)
	return nil
}

// Event handler for new events.
func (p *ParquetWriter) Event(ev *evs.Event, props map[string]string) error {

	obs := p.f.Convert(ev)

	err := p.writer.Write(obs)
	if (err != nil) {
		return err
	}

	return nil
	
}

func main() {

	p := &ParquetWriter{}

	binding, ok := os.LookupEnv("INPUT")
	if !ok {
		binding = "ioc"
	}

	err := p.Init(binding)
	if err != nil {
		log.Printf("Init: %v", err)
		return
	}

	log.Print("Initialisation complete.")

	p.Run()

}
