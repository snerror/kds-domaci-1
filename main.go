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
	"sort"
	"strings"
	"sync"
	"time"
)

type App struct {
	inputs []*FileInput
}

var app App

func main() {
	app = App{}

	// SETUP
	fi := NewFileInput("", []string{"data/disk1/A"})
	app.inputs = append(app.inputs, fi)

	o := NewOutput()
	c1 := NewCruncher(1, o)
	// c2 := NewCruncher(2, o)
	// c3 := NewCruncher(3, o)

	fi.Crunchers = append(fi.Crunchers, c1)
	// fi.Crunchers = append(fi.Crunchers, c2)
	// fi.Crunchers = append(fi.Crunchers, c3)
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
	go app.inputs[0].Start(ctx)

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
	DiskPath  string
	Dirs      []string
	Files     []*File
	Crunchers []*Cruncher
}

func NewFileInput(diskPath string, dirs []string) *FileInput {
	return &FileInput{
		DiskPath: diskPath,
		Dirs:     dirs,
	}
}

func (fi *FileInput) AddCruncher() {

}

func (fi *FileInput) Start(ctx context.Context) {
	scaner := make(chan *File)
	go fi.ScanDirs(scaner)

	for {
		f := <-scaner
		fmt.Println("FileInput.Start => file found", f.File.Name())
		go f.ReadFile(fi.Crunchers)
	}
}

func (fi *FileInput) ScanDirs(c chan *File) {
	for {
		for _, dir := range fi.Dirs {
			files, err := getFiles(dir)
			if err != nil {
				fmt.Printf("failed fetching files: %s\n", err)
				break
			}

			for i := range files {
				existing := (*File)(nil)
				for j := range fi.Files {
					if files[i].AbsolutePath == fi.Files[j].AbsolutePath {
						existing = fi.Files[j]
						break
					}
				}

				if existing == nil {
					fi.Files = append(fi.Files, &files[i])
					c <- &files[i]
					continue
				}

				if files[i].File.ModTime() != existing.File.ModTime() {
					fmt.Println("FileInput.ScanDirs => modified file found =>", existing.File.Name())
					existing.File = files[i].File
					c <- existing
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

func (f *File) ReadFile(crunchers []*Cruncher) {
	defer log.Printf("FileInput.ReadFile => finished reading %s", f.File.Name())

	fmt.Println("FileInput.ReadFile => opening file", f.File.Name())
	file, err := os.Open(f.AbsolutePath)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		fmt.Println("FileInput.ReadFile => closing file", f.File.Name())
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
		for _, c := range crunchers {
			c.Done <- c.GenerateCruncherFileName(f)
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		for _, c := range crunchers {
			c.Stream <- CruncherStream{c.GenerateCruncherFileName(f), fmt.Sprint(scanner.Text())}
		}
	}
}

type Cruncher struct {
	Arity    int
	Stream   chan CruncherStream
	Done     chan string
	Output   *Output
	Counters []*CruncherCounter
	Mutex    sync.Mutex
}

type CruncherStream struct {
	FileName string
	Text     string
}

func NewCruncher(arity int, output *Output) *Cruncher {
	cr := Cruncher{
		Arity:  arity,
		Stream: make(chan CruncherStream),
		Done:   make(chan string),
		Output: output,
	}
	go cr.Listen()
	return &cr
}

func (cr *Cruncher) Listen() {
	for {
		select {
		case s := <-cr.Stream:
			// fmt.Printf("Cruncher.Listen => cruncher arity %d => stream received from %s\n", cr.Arity, s.FileName)
			go cr.Consume(s)
			break
		case f := <-cr.Done:
			// fmt.Printf("Cruncher.Listen => cruncher arity %d => done received for %s\n", cr.Arity, f)
			cc := cr.GetCounter(f)
			cc.WaitGroup.Wait()
			go cc.WriteResults(cr.Output)
			break
		}
	}
}

func (cr *Cruncher) Consume(s CruncherStream) {
	words := strings.Fields(s.Text)
	// fmt.Printf("Cruncher.Consume => cruncher arity %d => words received %d\n", cr.Arity, len(words))
	cc := cr.GetOrCreateCounter(s.FileName)

	cc.WaitGroup.Add(1)
	cc.AddText(words)
}

func (cr *Cruncher) GetCounter(fn string) *CruncherCounter {
	found := (*CruncherCounter)(nil)
	for _, cc := range cr.Counters {
		if cc.FileName == fn {
			found = cc
			break
		}
	}
	return found
}

func (cr *Cruncher) GetOrCreateCounter(fn string) *CruncherCounter {
	cr.Mutex.Lock()
	cc := cr.GetCounter(fn)
	if cc == nil {
		cc = NewCruncherCounter(fn)
		cr.Counters = append(cr.Counters, cc)
	}
	cr.Mutex.Unlock()
	return cc
}

func (cr *Cruncher) GenerateCruncherFileName(f *File) string {
	return fmt.Sprintf("%s-arity%d", f.File.Name(), cr.Arity)
}

type CruncherCounter struct {
	FileName  string
	Mutex     sync.Mutex
	Data      map[string]int
	WaitGroup sync.WaitGroup
}

func NewCruncherCounter(fn string) *CruncherCounter {
	return &CruncherCounter{
		FileName: fn,
		Data:     make(map[string]int),
	}
}

func (cc *CruncherCounter) AddText(words []string) {
	defer cc.WaitGroup.Done()
	defer cc.Mutex.Unlock()

	cc.Mutex.Lock()
	for _, w := range words {
		if t, ok := cc.Data[w]; ok == false {
			cc.Data[w] = 1
		} else {
			cc.Data[w] = t + 1
		}
	}
}

func (cc *CruncherCounter) WriteResults(o *Output) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
		o.Done <- cc.FileName
		cc.Mutex.Unlock()
	}()
	cc.Mutex.Lock()

	type kv struct {
		Key   string
		Value int
	}

	var ss []kv
	for k, v := range cc.Data {
		ss = append(ss, kv{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	for _, s := range ss {
		o.Stream <- OutputStream{cc.FileName, fmt.Sprintf("%s %d\n", s.Key, s.Value)}
	}
}

type Output struct {
	Stream chan OutputStream
	Done   chan string
}

type OutputStream struct {
	FileName string
	Row      string
}

func NewOutput() *Output {
	o := &Output{
		Stream: make(chan OutputStream),
		Done:   make(chan string),
	}
	go o.Listen()
	return o
}

func (o *Output) Listen() {
	for {
		select {
		case s := <-o.Stream:
			// fmt.Printf("Output.Listen => signal received from %s with key %s\n", s.FileName, s.Row)
			if err := WriteToFile(s.FileName, s.Row); err != nil {
				fmt.Println("Output.Listen => stream => error =>", err)
			}
			break
		case d := <-o.Done:
			fmt.Printf("Output.Listen => done received from %s\n", d)
			if err := FinishFile(d); err != nil {
				fmt.Println("Output.Listen => done => error =>", err)
			}
			break
		}
	}
}

func WriteToFile(fn string, row string) error {
	path := fmt.Sprintf("./output/%s-tmp", fn)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed creating or opening file: %s", err)
	}

	defer f.Close()

	if _, err = f.WriteString(row); err != nil {
		return fmt.Errorf("failed to write to file: %s", err)
	}

	return nil
}

func FinishFile(fn string) error {
	oldPath := fmt.Sprintf("./output/%s-tmp", fn)
	newPath := fmt.Sprintf("./output/%s", fn)

	if err := os.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("failed to rename file: %s", err)
	}

	return nil
}
