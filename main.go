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
	"strconv"
	"strings"
	"sync"
	"time"
)

type App struct {
	inputs    []*FileInput
	crunchers []*Cruncher
	disks     []*Disk
}

func (app *App) GetCruncherByArity(a int) *Cruncher {
	for _, c := range app.crunchers {
		if c.Arity == a {
			return c
		}
	}

	return nil
}

func (app *App) GetDiskByPath(path string) *Disk {
	for _, disk := range app.disks {
		if disk.Path == path {
			return disk
		}
	}
	return nil
}

var app App
var o = NewOutput()
var L = 10

func main() {
	app = App{}

	// SETUP
	d := NewDisk("E:\\Workspace\\kds-domaci-1\\data\\disk1")
	fi := NewFileInput(1, d, []string{"A", "B"})
	app.inputs = append(app.inputs, fi)

	c1 := NewCruncher(1)
	// c2 := NewCruncher(2)
	// c3 := NewCruncher(3)

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
	// c := make(chan string)
	// go CliInput(c)

APP:
	for {
		select {
		// case command := <-c:
		// 	ExecuteCommand(command)
		// 	break
		case <-ctx.Done():
			fmt.Println("Exiting application.")
			break APP
		}
	}

	return nil
}

func CliInput(cli chan string) {
	for {
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		cli <- text
	}
}

func ExecuteCommand(command string) {
	tokens := strings.Fields(command)
	if len(tokens) == 0 {
		return
	}
	switch tokens[0] {
	case "help":
		HelpCommand()
		break
	case "cr":
		CruncherCommand(tokens)
		break
	case "fi":
		FileInputCommand(tokens)
		break
	default:
		fmt.Println("Unknown command")
		break
	}
}

func HelpCommand() {
	fmt.Println("Commands:")
	fmt.Println("help								Prints help")
	fmt.Println("cr <number>							Add cruncher")
	fmt.Println("cr ls								List crunchers")
	fmt.Println("fi ls								List file inputs")
	fmt.Println("fi <disk> <dir1,dir2> <cruncher_1,cruncher_2>		Add file input")
}

func CruncherCommand(tokens []string) {
	if len(tokens) != 2 {
		fmt.Println("Invalid arguments")
		return
	}
	if tokens[1] == "ls" {
		if len(app.crunchers) == 0 {
			fmt.Println("No crunchers added")
			return
		}
		for _, c := range app.crunchers {
			fmt.Printf("cruncher %d\n", c.Arity)
		}
		return
	}
	if arity, err := strconv.Atoi(tokens[1]); err == nil {
		// TODO check if cruncher of arity exists
		c := NewCruncher(arity)
		app.crunchers = append(app.crunchers, c)
		fmt.Printf("Cruncher with arity %d added \n", c.Arity)

	} else {
		fmt.Println("Failed adding cruncher")
	}
}

func FileInputCommand(tokens []string) {
	if len(tokens) < 2 {
		fmt.Println("Invalid arguments")
		return
	}

	if tokens[1] == "ls" {
		if len(app.inputs) == 0 {
			fmt.Println("No file inputs added")
			return
		}
		for _, i := range app.inputs {
			fmt.Printf("file input %d\n", i.Id)
		}
		return
	}

	if len(tokens) < 4 {
		fmt.Println("Invalid arguments")
		return
	}

	disk := tokens[1]
	dirs := strings.Split(tokens[2], ",")
	fmt.Println(dirs)
	crunchers := strings.Split(tokens[3], ",")

	d := app.GetDiskByPath(disk)
	if d == nil {
		d = NewDisk(disk)
	}

	fi := NewFileInput(len(app.inputs)+1, d, dirs)
	for _, a := range crunchers {
		if t, err := strconv.Atoi(a); err == nil {
			if c := app.GetCruncherByArity(t); c != nil {
				fi.Crunchers = append(fi.Crunchers, c)
			} else {
				fmt.Println("Invalid cruncher id", a)
			}
		}
	}
	app.inputs = append(app.inputs, fi)
	fmt.Println("Added file input with id", fi.Id)
}

type Disk struct {
	Path  string
	Mutex sync.Mutex
}

func NewDisk(path string) *Disk {
	return &Disk{
		Path: path,
	}
}

type File struct {
	AbsolutePath string
	File         fs.FileInfo
	Disk         *Disk
}

func NewFile(path string, file fs.FileInfo) *File {
	return &File{
		AbsolutePath: path,
		File:         file,
	}

}

type FileInput struct {
	Id        int
	Disk      *Disk
	Dirs      []string
	Files     []*File
	Crunchers []*Cruncher
}

func NewFileInput(id int, disk *Disk, dirs []string) *FileInput {
	return &FileInput{
		Id:   id,
		Disk: disk,
		Dirs: dirs,
	}
}

func (fi *FileInput) Start(ctx context.Context) {
	scaner := make(chan *File)
	go fi.ScanDirs(scaner)

	for {
		// SEKVENCIJALNO AKO SU SA ISTOG DISKA
		f := <-scaner
		fmt.Println("FileInput => Start => file found", f.File.Name())
		go f.ReadFile(fi.Crunchers)
	}
}

func (fi *FileInput) ScanDirs(c chan *File) {
	for {
		for _, dir := range fi.Dirs {
			files, err := getFiles(fmt.Sprintf("%s/%s", fi.Disk.Path, dir))
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
					files[i].Disk = fi.Disk
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

		files = append(files, *NewFile(absoluteFilePath, fileInfo))
		return nil
	}); err != nil {
		return nil, err
	}

	return files, nil
}

func (f *File) ReadFile(crunchers []*Cruncher) {
	defer func() {
		fmt.Println("FileInput => ReadFile => finished readings", f.File.Name())
		fmt.Printf("FileInput => ReadFile => unlocking disk %s for file %s\n", f.Disk.Path, f.File.Name())
		f.Disk.Mutex.Unlock()
		for _, c := range crunchers {
			c.Done <- c.GenerateCruncherFileName(f) // TODO ovo je redudantno kad zavrim nove izmene
		}
	}()

	f.Disk.Mutex.Lock()
	fmt.Printf("FileInput => ReadFile => locking disk %s for file %s\n", f.Disk.Path, f.File.Name())

	fmt.Println("FileInput => ReadFile => opening file", f.File.Name())
	file, err := os.Open(f.AbsolutePath)
	if err != nil {
		fmt.Printf("FileInput => Read File => failed reading file %s: %s\n", f.File.Name(), err)
		return
	}

	defer func() {
		fmt.Println("FileInput => ReadFile => closing file", f.File.Name())
		if err = file.Close(); err != nil {
			fmt.Printf("FileInput => Read File => failed closing file %s: %s\n", f.File.Name(), err)
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

type KeyValue struct {
	Key   string
	Value int
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
	}()

	var ss []*KeyValue
	for k, v := range cc.Data {
		ss = append(ss, &KeyValue{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})

	fmt.Printf("CruncherCounter => WriteResults => started streaming to output for %s\n", cc.FileName)
	o.Stream <- OutputStream{cc.FileName, ss}
}

type Output struct {
	Stream chan OutputStream
	Done   chan string
}

type OutputStream struct {
	FileName string
	Data     []*KeyValue
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
			go o.WriteToFile(&s)
			break
		case d := <-o.Done:
			fmt.Printf("Output => Listen => finished writing file %s\n", d)
			if err := FinishFile(d); err != nil {
				fmt.Println("Output => Listen => done => error =>", err)
			}
			break
		}
	}
}

func (o *Output) WriteToFile(s *OutputStream) {
	path := fmt.Sprintf("./output/%s-tmp", s.FileName)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Outpuit => WriteToFile => error while writing ", r)

		}
		o.Done <- s.FileName
	}()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("failed creating or opening file: %s\n", err)
		return
	}

	defer f.Close()

	for _, s := range s.Data {
		if _, err = f.WriteString(fmt.Sprintf("%s %d\n", s.Key, s.Value)); err != nil {
			fmt.Printf("failed to write to file: %s\n", err)
			return
		}
	}
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
