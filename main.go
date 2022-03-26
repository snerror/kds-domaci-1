package main

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"time"
)

type App struct {
	inputs []*FileInput
}

var app App

func main() {
	app = App{}

	// SETUP
	fi := FileInput{
		Id:       1,
		DiskPath: "",
		Dirs:     []string{"data/disk1/A"},
	}
	app.inputs = append(app.inputs, &fi)
	c1 := NewCruncher(1)
	fi.Crunchers = append(fi.Crunchers, c1)

	// END SETUP

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

var reader = bufio.NewReader(os.Stdin)

func readKey(input chan rune) {
	char, _, err := reader.ReadRune()
	if err != nil {
		log.Fatal(err)
	}
	input <- char
}

func run(ctx context.Context) error {
	fmt.Println("Application up.")

	go app.inputs[0].Start()
APP:
	for {
		<-ctx.Done()
		fmt.Println("Exiting application.")
		break APP
	}

	return nil
}

type File struct {
	AbsolutePath string
	File         fs.FileInfo
	Finished     bool
}

type FileInput struct {
	Id        int
	DiskPath  string
	Dirs      []string
	Files     []*File
	Crunchers []*Cruncher
}

func (fi *FileInput) Start() {
	scaner := make(chan *File)
	go fi.ScanDirs(scaner)

	for {
		f := <-scaner
		fmt.Println("file found", f.File.Name())
		go f.ReadFile(fi.Crunchers[0].stream, fi.Crunchers[0].done)
	}
}

func (fi *FileInput) ScanDirs(c chan *File) error {
	for {
		for _, dir := range fi.Dirs {
			files, err := getFiles(dir)
			if err != nil {
				return err
			}

			for i := range files {
				found := (*File)(nil)
				for j := range fi.Files {
					if files[i].AbsolutePath == fi.Files[j].AbsolutePath {
						found = &files[i]
						break
					}
				}

				if found == nil {
					fi.Files = append(fi.Files, &files[i])
					c <- &files[i]
					// c <- fmt.Sprintf("input %d found new file %s\n", fi.Id, files[i].File.Name())
				} else {
					// c <- fmt.Sprintln("file already added: ", f.AbsolutePath)
					// if f.File.ModTime() != found.File.ModTime() {
					// 	// TODO check for modifications
					// }
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func getFiles(dir string) ([]File, error) {
	var files []File

	if err := filepath.Walk(dir, func(path string, fileInfo fs.FileInfo, err error) error {
		if fileInfo.IsDir() {
			return nil
		}
		_, err = regexp.MatchString(".txt", fileInfo.Name())
		if err != nil {
			return err
		}

		absoluteFilePath, err := filepath.Abs(path)
		if err != nil {
			return err
		}

		files = append(files, File{
			AbsolutePath: absoluteFilePath,
			File:         fileInfo,
			Finished:     false,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func (f *File) ReadFile(stream chan CruncherStream, done chan string) {
	if f.Finished {
		done <- f.AbsolutePath
		return
	}

	defer log.Printf("Finished reading %s", f.File.Name())
	fmt.Println("opening file ", f.File.Name())
	file, err := os.Open(f.AbsolutePath)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		fmt.Println("closing file ", f.File.Name())
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		stream <- CruncherStream{f.AbsolutePath, fmt.Sprint(scanner.Text())}
	}

	done <- f.AbsolutePath
}

type Cruncher struct {
	arity    int
	stream   chan CruncherStream
	done     chan string
	counters []*CruncherCounter
}

type CruncherStream struct {
	fileName string
	text     string
}

type CruncherCounter struct {
	fileName string
	counter  uint32
}

func NewCruncher(arity int) *Cruncher {
	cr := Cruncher{
		arity:  arity,
		stream: make(chan CruncherStream),
		done:   make(chan string),
	}
	go cr.Listen()
	return &cr
}

func (cr *Cruncher) Listen() {
	for {
		select {
		case cs := <-cr.stream:
			cr.Count(cs)
			break
		case d := <-cr.done:
			cc := cr.GetCruncherCounter(d)
			if cc == nil {
				fmt.Printf("cound not find counter %s\n", d)
				break
			}
			fmt.Printf("done %s with word count %d\n", cc.fileName, cc.counter)
			break
		}
	}
}

func (cr *Cruncher) Count(stream CruncherStream) error {
	cc := cr.GetCruncherCounter(stream.fileName)

	if cc == nil {
		cc := NewCruncherCounter(stream.fileName)
		cr.counters = append(cr.counters, cc)
		cc.CountWords(stream.text)
	} else {
		cc.CountWords(stream.text)
	}

	return nil
}

func (cr *Cruncher) GetCruncherCounter(fileName string) *CruncherCounter {
	found := (*CruncherCounter)(nil)
	for _, cc := range cr.counters {
		if cc.fileName == fileName {
			found = cc
		}
	}
	return found
}

func NewCruncherCounter(fileName string) *CruncherCounter {
	return &CruncherCounter{
		fileName: fileName,
		counter:  0,
	}

}

func (cc *CruncherCounter) CountWords(text string) error {
	atomic.AddUint32(&cc.counter, uint32(len(text)))
	return nil
}
