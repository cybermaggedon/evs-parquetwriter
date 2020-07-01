//
// ElasticSearch loader.  Maps events to an ElasticSearch schema.
//

package main

import (
	"github.com/cybermaggedon/evs-golang-api"
	pb "github.com/cybermaggedon/evs-golang-api/protos"
	"log"
)

type ParquetWriter struct {

	// Configuration
	*PWConfig

	// Event analytic framework
	*evs.EventSubscriber
	*evs.EventProducer
	evs.Interruptible

	// Flattener turns events into parquet model
	f Flattener

	// Parquet writer
	writer *Writer
}

// Initialisation
func NewParquetWriter(pwc *PWConfig) *ParquetWriter {

	p := &ParquetWriter{PWConfig: pwc}

	var err error
	p.EventSubscriber, err = evs.NewEventSubscriber(p, p)
	if err != nil {
		log.Fatal(err)
	}

	p.EventProducer, err = evs.NewEventProducer(p)
	if err != nil {
		log.Fatal(err)
	}

	p.RegisterStop(p)

	p.writer, err = p.Build()
	if err != nil {
		log.Fatal(err)
	}

	return p
}

// Event handler for new events.
func (p *ParquetWriter) Event(ev *pb.Event, props map[string]string) error {

	obs := p.f.Convert(ev)

	err := p.writer.Write(obs)
	if err != nil {
		return err
	}

	return nil

}

func (p *ParquetWriter) Stop() {
	p.writer.Close()
	log.Print("Closed parquet file")
	p.EventSubscriber.Stop()
}

func main() {

	gc := NewPWConfig()

	g := NewParquetWriter(gc)

	log.Print("Initialisation complete")

	g.Run()
	log.Print("Shutdown.")

}
