package main

// Parquet file writer.

import (
	"time"
	"log"
	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/parquet"
	"path/filepath"
	"os"
)

type Writer struct {
	WriterConfig
	file  source.ParquetFile
	writer *writer.ParquetWriter

	batch_count uint64
	batch_start time.Time
}

func (w *Writer) NewPath() string {

	//create a new bucket storage path
	tm := time.Now().Format("2006-01-02/15-04")
	uid := uuid.New().String()
	path := w.directory + "/" + tm + "/" + uid + ".parquet"

	return path

}

func (w *Writer) OpenFile() error {

	path := w.NewPath()

	// Create directory if not exists
	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}

	file, err := local.NewLocalFileWriter(path)
	if err != nil {
		return err
	}

	log.Print("Started writing new file ", path)

	// Parquet writer
	writer, err := writer.NewParquetWriter(file, new(FlatEvent),
		w.output_threads)
	if err != nil {
		file.Close()
		return err
	}

	writer.RowGroupSize = 128 * 1024 * 1024 // 128M
	writer.CompressionType = parquet.CompressionCodec_SNAPPY

	w.file = file
	w.writer = writer

	w.batch_count = 0
	w.batch_start = time.Now()

	return nil

}

func (wc WriterConfig) Build() (*Writer, error) {

	w := &Writer{
		WriterConfig: wc,
	}

	err := w.OpenFile()

	if err != nil {
		return nil, err
	}

	return w, nil

}

func (w *Writer) Close() error {

	err := w.writer.WriteStop()
	if err != nil {
		w.file.Close()
		return err
	}

	w.file.Close()

	return nil

}

func (w *Writer) Write(d interface{}) error {

	if (uint64(w.batch_count) > w.flush_count) ||
		(time.Since(w.batch_start) > w.flush_interval) {

		err := w.Close()
		if err != nil {
			return err
		}

		err = w.OpenFile()
		if err != nil {
			return err
		}

	}

	err := w.writer.Write(d)
	if err != nil {
		return err
	}

	w.batch_count += 1

	return nil

}
