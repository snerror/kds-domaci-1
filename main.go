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
var o = NewOutput()

var L = 10

func main() {
	app = App{}

	// SETUP
	fi := NewFileInput("", []string{"data/disk1/C"})
	app.inputs = append(app.inputs, fi)

	// o := NewOutput()
	c1 := NewCruncher(1)
	// c2 := NewCruncher(2)
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
		// SEKVENCIJALNO AKO SU SA ISTOG DISKA
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
	defer func() {
		log.Printf("FileInput.ReadFile => finished reading %s", f.File.Name())
		for _, c := range crunchers {
			c.Done <- c.GenerateCruncherFileName(f) // TODO ovo je redudantno kad zavrim nove izmene
		}
	}()

	fmt.Println("FileInput.ReadFile => opening file", f.File.Name())
	file, err := os.Open(f.AbsolutePath)
	if err != nil {
		fmt.Printf("failed reading file %s: %s\n", f.File.Name(), err)
		return
	}

	defer func() {
		fmt.Println("FileInput.ReadFile => closing file", f.File.Name())
		if err = file.Close(); err != nil {
			fmt.Printf("failed closing file %s: %s\n", f.File.Name(), err)
		}
	}()

	text := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text += scanner.Text()
	}

	for _, c := range crunchers {
		c.Stream <- CruncherStream{c.GenerateCruncherFileName(f), fmt.Sprint(text)}
	}
}

type Cruncher struct {
	Arity    int
	Stream   chan CruncherStream
	Done     chan string
	Counters []*CruncherCounter
	Mutex    sync.Mutex
}

type CruncherStream struct {
	FileName string
	Text     string
}

func NewCruncher(arity int) *Cruncher {
	cr := Cruncher{
		Arity:  arity,
		Stream: make(chan CruncherStream),
		Done:   make(chan string),
	}
	go cr.Listen()
	return &cr
}

func (cr *Cruncher) CrunchFile(s *CruncherStream) {
	words := strings.Fields(s.Text)
	divided := splitText(words, L)

	cc := cr.GetOrCreateCounter(s.FileName)
	for _, d := range divided {
		go cc.CountSegment(s.FileName, d)
	}
	cc.WriteResults(o)
}

func (cr *Cruncher) Listen() {
	for {
		s := <-cr.Stream
		go cr.CrunchFile(&s)
		break
	}
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
		cc = NewCruncherCounter(fn, cr.Arity)
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
	Arity     int
	Done      chan (map[string]int)
	Queue     []string
	Mutex     sync.Mutex
	Data      map[string]int
	WaitGroup sync.WaitGroup
}

func NewCruncherCounter(fn string, arity int) *CruncherCounter {
	cc := &CruncherCounter{
		FileName: fn,
		Arity:    arity,
		Data:     make(map[string]int),
		Done:     make(chan map[string]int),
	}
	go cc.Listen()
	return cc
}

func (cc *CruncherCounter) CountSegment(fn string, words []string) {
	cc.WaitGroup.Add(1)
	m := make(map[string]int)

	for len(words) > cc.Arity {
		w := strings.Join(words[0:cc.Arity], " ")
		words = words[1:]

		if t, ok := m[w]; ok == false {
			m[w] = 1
		} else {
			m[w] = t + 1
		}
	}
	fmt.Printf("CruncherCounter => CountSegment => finished counting segment with result length %d for %s\n", len(m), fn)
	cc.Done <- m
}

func (cc *CruncherCounter) Listen() {
	for {
		m := <-cc.Done
		mergeMaps(cc.Data, m)
		cc.WaitGroup.Done()
	}
}

func (cc *CruncherCounter) WriteResults(o *Output) {
	fmt.Printf("CruncherCounter => WriteResults => waiting to finish all jobs for %s\n", cc.FileName)
	cc.WaitGroup.Wait()
	fmt.Printf("CruncherCounter => WriteResults => finished waiting for %s\n", cc.FileName)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
		o.Done <- cc.FileName
	}()

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

	fmt.Printf("CruncherCounter => WriteResults => started streaming to output for %s\n", cc.FileName)
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
				fmt.Println("Output => Listen => stream => error =>", err)
			}
			break
		case d := <-o.Done:
			fmt.Printf("Output => Listen => done received from %s\n", d)
			if err := FinishFile(d); err != nil {
				fmt.Println("Output => Listen => done => error =>", err)
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

func splitText(words []string, num int) [][]string {
	var divided [][]string

	chunkSize := (len(words) + num - 1) / num

	for i := 0; i < len(words); i += chunkSize {
		end := i + chunkSize

		if end > len(words) {
			end = len(words)
		}

		divided = append(divided, words[i:end])
	}

	return divided
}

func mergeMaps(m1 map[string]int, m2 map[string]int) map[string]int {
	for k, v := range m2 {
		if _, ok := m1[k]; ok == false {
			m1[k] = v
		} else {
			m1[k] += v
		}
	}
	return m1
}
