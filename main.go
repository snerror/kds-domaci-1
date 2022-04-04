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

	"github.com/spf13/viper"
)

type Config struct {
	Disks              string `mapstructure:"disks"`
	CounterLimit       int    `mapstructure:"counter_data_limit"`
	FileInputSleepTime int    `mapstructure:"file_input_sleep_time"`
	SlowWrite          bool   `mapstructure:"slow_write"`
}

func LoadConfig() (*Config, error) {
	viper.AddConfigPath(".")
	viper.SetConfigName("app")
	viper.SetConfigType("properties")

	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	config := Config{}
	if err = viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

type App struct {
	Inputs             []*FileInput
	Crunchers          []*Cruncher
	Disks              []*Disk
	CounterLimit       int
	FileInputSleepTime int
	SlowWrite          bool
}

func (app *App) GetCruncherByArity(a int) *Cruncher {
	for _, c := range app.Crunchers {
		if c.Arity == a {
			return c
		}
	}

	return nil
}

func (app *App) LoadConfig(c *Config) {
	disks := strings.Split(c.Disks, ";")
	for _, d := range disks {
		app.Disks = append(app.Disks, NewDisk(d))
	}
	app.CounterLimit = c.CounterLimit
	app.FileInputSleepTime = c.FileInputSleepTime
	app.SlowWrite = c.SlowWrite
}

func (app *App) GetDiskByPath(path string) *Disk {
	for _, disk := range app.Disks {
		if disk.Path == path {
			return disk
		}
	}
	return nil
}

func (app *App) GetOrCreateDisk(path string) *Disk {
	d := app.GetDiskByPath(path)
	if d == nil {
		d = NewDisk(path)
		app.Disks = append(app.Disks, d)
	}
	return d
}

func (app *App) GetFileInputById(id int) *FileInput {
	found := (*FileInput)(nil)
	for _, fi := range app.Inputs {
		if fi.Id == id {
			found = fi
			break
		}
	}
	return found
}

var app App
var o = NewOutput()

func main() {
	app = App{}

	config, err := LoadConfig()
	if err != nil {
		log.Fatal(err)
		return
	}
	app.LoadConfig(config)

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	fmt.Println("Application up.")
	HelpCommand()

	c := make(chan string)
	go CliInput(c)

APP:
	for {
		select {
		case command := <-c:
			ExecuteCommand(command)
			break
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
		go HelpCommand()
		break
	case "start":
		if len(app.Inputs) == 0 {
			fmt.Println("No inputs registered.")
			break
		}
		for _, i := range app.Inputs {
			go i.Start()
		}
		break
	case "scenario":
		go ScenarioCommand(tokens)
		break
	case "cr":
		go CruncherCommand(tokens)
		break
	case "fi":
		go FileInputCommand(tokens)
		break
	case "poll":
		go PrintCommand(tokens, false)
		break
	case "take":
		PrintCommand(tokens, true)
		break
	case "merge":
		go MergeCommand(tokens)
		break
	case "outputs":
		for _, of := range o.OutputFile {
			fmt.Println(of.FileName)
		}
		break
	default:
		fmt.Println("Unknown command")
		break
	}
}

func HelpCommand() {
	fmt.Println("Commands:")
	fmt.Println("help								Prints help")
	fmt.Println("start								Starts counter")
	fmt.Println("poll <filename>							Prints file if able")
	fmt.Println("take <filename>							Prints file (waits if needed)")
	fmt.Println("merge <filename1, filename2...>					Merge two results")
	fmt.Println("scenario							Register file inputs and crunchers for scenario")
	fmt.Println("cr <number>							Add cruncher")
	fmt.Println("cr ls								List crunchers")
	fmt.Println("fi ls								List file inputs")
	fmt.Println("fi create <disk>						Creates FileInput for disk")
	fmt.Println("fi cr add <fileinput> <cruncher>				Adds cruncher to FileInput")
	fmt.Println("fi cr remove <fileinput> <cruncher>				Removes cruncher from FileInput")
	fmt.Println("fi dir add <fileinput> <dir>					Adds dir to FileInput")
	fmt.Println("fi dir remove <fileinput> <dir>					Removes dir from FileInput")
	fmt.Println("outputs								Lists all output files")
}

func ScenarioCommand(tokens []string) {
	if len(tokens) == 1 {
		fmt.Println("Invalid number of arguments")
		return
	}
	switch tokens[1] {
	case "1":
		fi1 := NewFileInput(1, app.Disks[0])
		app.Inputs = append(app.Inputs, fi1)

		c1 := NewCruncher(1)
		app.Crunchers = append(app.Crunchers, c1)

		fi1.AddCruncher(c1)
		fi1.AddDir("A")

		fmt.Println("Scenario 1 loaded")
		break
	case "2":
		fi1 := NewFileInput(1, app.Disks[0])
		fi2 := NewFileInput(2, app.Disks[1])
		app.Inputs = append(app.Inputs, fi1)
		app.Inputs = append(app.Inputs, fi2)

		c1 := NewCruncher(1)
		app.Crunchers = append(app.Crunchers, c1)

		fi1.AddCruncher(c1)
		fi2.AddCruncher(c1)

		fmt.Println("Scenario 2 loaded")
		break
	case "3":
		fi1 := NewFileInput(1, app.Disks[0])
		app.Inputs = append(app.Inputs, fi1)

		c1 := NewCruncher(1)
		app.Crunchers = append(app.Crunchers, c1)
		c2 := NewCruncher(2)
		app.Crunchers = append(app.Crunchers, c2)

		fi1.AddCruncher(c1)
		fi1.AddCruncher(c2)
		fmt.Println("Scenario 3 loaded")
		break
	case "4":
		fi1 := NewFileInput(1, app.Disks[0])
		app.Inputs = append(app.Inputs, fi1)

		c1 := NewCruncher(1)
		app.Crunchers = append(app.Crunchers, c1)
		c2 := NewCruncher(2)
		app.Crunchers = append(app.Crunchers, c2)

		fi1.AddCruncher(c1)
		fmt.Println("Scenario 4 loaded")
		break
	case "overkill":
		fi1 := NewFileInput(1, app.Disks[0])
		app.Inputs = append(app.Inputs, fi1)
		fi2 := NewFileInput(2, app.Disks[0])
		app.Inputs = append(app.Inputs, fi2)
		fi3 := NewFileInput(3, app.Disks[1])
		app.Inputs = append(app.Inputs, fi3)
		fi4 := NewFileInput(4, app.Disks[1])
		app.Inputs = append(app.Inputs, fi4)

		c1 := NewCruncher(1)
		app.Crunchers = append(app.Crunchers, c1)
		c2 := NewCruncher(2)
		app.Crunchers = append(app.Crunchers, c2)
		c3 := NewCruncher(3)
		app.Crunchers = append(app.Crunchers, c3)
		c4 := NewCruncher(4)
		app.Crunchers = append(app.Crunchers, c4)
		c5 := NewCruncher(5)
		app.Crunchers = append(app.Crunchers, c5)

		fi1.AddCruncher(c1)
		fi1.AddCruncher(c2)
		fi1.AddCruncher(c3)
		fi1.AddCruncher(c4)
		fi1.AddCruncher(c5)
		fi2.AddCruncher(c1)
		fi2.AddCruncher(c2)
		fi2.AddCruncher(c3)
		fi2.AddCruncher(c4)
		fi2.AddCruncher(c5)
		fi3.AddCruncher(c1)
		fi3.AddCruncher(c2)
		fi3.AddCruncher(c3)
		fi3.AddCruncher(c4)
		fi3.AddCruncher(c5)
		fi4.AddCruncher(c1)
		fi4.AddCruncher(c2)
		fi4.AddCruncher(c3)
		fi4.AddCruncher(c4)
		fi4.AddCruncher(c5)

		fmt.Println("Scenario overkill loaded")
		break
	default:
		fmt.Println("Unknown scenario")
	}

}

func CruncherCommand(tokens []string) {
	if len(tokens) != 2 {
		fmt.Println("Invalid arguments")
		return
	}
	if tokens[1] == "ls" {
		if len(app.Crunchers) == 0 {
			fmt.Println("No crunchers added")
			return
		}
		for _, c := range app.Crunchers {
			fmt.Printf("cruncher %d\n", c.Arity)
		}
		return
	}
	if arity, err := strconv.Atoi(tokens[1]); err == nil {
		// TODO check if cruncher of arity exists
		c := NewCruncher(arity)
		app.Crunchers = append(app.Crunchers, c)
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

	switch tokens[1] {
	case "ls":
		goto FIList
	case "create":
		goto FICreate
	case "cr":
		goto FICruncher
	case "dir":
		goto FIDir
	default:
		fmt.Println("Unknown command")

	}

FIList:
	{
		if len(app.Inputs) == 0 {
			fmt.Println("No file inputs added")
			return
		}
		for _, i := range app.Inputs {
			fmt.Printf("FILE INPUT\n")
			fmt.Printf("- Id => %d\n", i.Id)
			fmt.Printf("- DiskPath => %s\n", i.Disk.Path)
			fmt.Printf("- Dirs => %d\n", len(i.Dirs))
			fmt.Printf("- Crunchers => %d\n", len(i.Crunchers))
		}
		return
	}
FICreate:
	{
		if len(tokens) != 3 {
			fmt.Println("Missing dirs")
			return
		}
		d := app.GetOrCreateDisk(tokens[2])
		fi := NewFileInput(len(app.Inputs)+1, d)
		app.Inputs = append(app.Inputs, fi)
		fmt.Println("FileInput added")
		return
	}
FICruncher:
	{
		if len(tokens) != 5 {
			fmt.Println("Missing args")
			return
		}

		fiId, err := strconv.Atoi(tokens[3])
		if err != nil {
			fmt.Println("Unknown FileInput Id")
			return
		}
		crAr, err := strconv.Atoi(tokens[4])
		if err != nil {
			fmt.Println("Unknown Cruncher Arity")
			return
		}

		fi := app.GetFileInputById(fiId)
		if fi == nil {
			fmt.Println("No FileInput found with id", fiId)
			return
		}
		cr := app.GetCruncherByArity(crAr)
		if cr == nil {
			fmt.Println("No cruncher found with arity", crAr)
			return
		}

		switch tokens[2] {
		case "remove":
			fi.RemoveCruncher(cr)
			break
		case "add":
			fi.AddCruncher(cr)
			break
		default:
			fmt.Println("Unknown command")
		}
		return
	}
FIDir:
	{
		if len(tokens) != 5 {
			fmt.Println("Missing args")
			return
		}

		fiId, err := strconv.Atoi(tokens[3])
		if err != nil {
			fmt.Println("Unknown FileInput Id")
			return
		}

		fi := app.GetFileInputById(fiId)
		if fi == nil {
			fmt.Println("No FileInput found with id", fiId)
			return
		}

		dir := tokens[4]

		switch tokens[2] {
		case "remove":
			fi.RemoveDir(dir)
			break
		case "add":
			fi.AddDir(dir)
			break
		default:
			fmt.Println("Unknown command")
		}
		return
	}
}

func PrintCommand(tokens []string, block bool) {
	if len(tokens) < 2 {
		fmt.Println("Invalid arguments")
		return
	}

	of := o.GetOutputFile(tokens[1])

	if of == nil {
		fmt.Println("File not found")
		return
	}

	defer of.Mutex.Unlock()

	if block {
		fmt.Printf("Waiting for file %s to be ready\n", of.FileName)
		of.Mutex.Lock()
	} else {
		if of.Mutex.TryLock() == false {
			fmt.Printf("File %s is currently locked. Try again later\n", of.FileName)
			return
		}
	}

	path := fmt.Sprintf("./output/%s", of.FileName)

	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Failed opening file:", err)
		return
	}

	defer func() {
		if err = file.Close(); err != nil {
			fmt.Println("Failed closing file:", err)
		}
	}()

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		if i >= 20 {
			break
		}
		fmt.Println(scanner.Text())
		i++
	}
}

func MergeCommand(tokens []string) {
	if len(tokens) < 2 {
		fmt.Println("Invalid arguments")
		return
	}

	o.MergeFiles(tokens[1])
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

func NewFileInput(id int, disk *Disk) *FileInput {
	return &FileInput{
		Id:   id,
		Disk: disk,
		Dirs: make([]string, 0),
	}
}

func (fi *FileInput) IsCruncherAdded(cr *Cruncher) bool {
	for _, c := range fi.Crunchers {
		if c == cr {
			return true
		}
	}
	return false
}

func (fi *FileInput) AddCruncher(cr *Cruncher) {
	if c := fi.IsCruncherAdded(cr); c == true {
		fmt.Printf("Cruncher %d already added to FileInput %d\n", cr.Arity, fi.Id)
		return
	}
	fi.Crunchers = append(fi.Crunchers, cr)
	fmt.Printf("Cruncher %d added to FileInput %d\n", cr.Arity, fi.Id)
}

func (fi *FileInput) RemoveCruncher(cr *Cruncher) {
	if c := fi.IsCruncherAdded(cr); c == false {
		fmt.Printf("Cruncher %d is not added to FileInput %d\n", cr.Arity, fi.Id)
		return
	}

	for i, c := range fi.Crunchers {
		if c == cr {
			fi.Crunchers = append(fi.Crunchers[:i], fi.Crunchers[i+1:]...)
		}
	}
	fmt.Printf("Cruncher %d removed from FileInput %d\n", cr.Arity, fi.Id)
}

func (fi *FileInput) IsDirAdded(dir string) bool {
	for _, d := range fi.Dirs {
		if dir == d {
			return true
		}
	}
	return false
}

func (fi *FileInput) AddDir(dir string) {
	if d := fi.IsDirAdded(dir); d == true {
		fmt.Printf("Dir %s already added to FileInput %d\n", dir, fi.Id)
		return
	}
	fi.Dirs = append(fi.Dirs, dir)
	fmt.Printf("Dir %s added to FileInput %d\n", dir, fi.Id)
}

func (fi *FileInput) RemoveDir(dir string) {
	if d := fi.IsDirAdded(dir); d == false {
		fmt.Printf("Dir %s is not added to FileInput %d\n", dir, fi.Id)
		return
	}
	for i, d := range fi.Dirs {
		if d == dir {
			fi.Dirs = append(fi.Dirs[:i], fi.Dirs[i+1:]...)
		}
	}
	fmt.Printf("Dir %s removed from FileInput %d\n", dir, fi.Id)
}

func (fi *FileInput) Start() {
	scaner := make(chan *File)
	if len(fi.Crunchers) == 0 {
		fmt.Println("!Warning! No crunchers added to FileInput")
	}
	go fi.Scanner(scaner)

	for {
		f := <-scaner
		fmt.Println("FileInput => Start => file found", f.File.Name())
		go f.ReadFile(fi.Crunchers)
	}
}

func (fi *FileInput) Scanner(c chan *File) {
	for {
		if len(fi.Dirs) > 0 {
			for _, dir := range fi.Dirs {
				fi.ScanDir(dir, c)
			}
		} else {
			fi.ScanDir(".", c)
		}
		time.Sleep(time.Millisecond * time.Duration(app.FileInputSleepTime))
	}
}

func (fi *FileInput) ScanDir(dir string, c chan *File) {
	files, err := getFiles(fmt.Sprintf("%s/%s", fi.Disk.Path, dir))
	if err != nil {
		fmt.Printf("failed fetching files: %s\n", err)
		return
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
	f.Disk.Mutex.Lock()
	fmt.Printf("FileInput => ReadFile => locking disk %s for file %s\n", f.Disk.Path, f.File.Name())

	text, err := readFile(f.AbsolutePath)
	if err != nil {
		fmt.Printf("FileInput => ReadFile => failed reading file %s: %s\n", f.File.Name(), err)
		f.Disk.Mutex.Unlock()
		return
	}

	for _, c := range crunchers {
		c.Stream <- CruncherStream{c.GenerateCruncherFileName(f), text}
	}

	fmt.Printf("FileInput => ReadFile => unlocking disk %s for file %s\n", f.Disk.Path, f.File.Name())

	f.Disk.Mutex.Unlock()
	for _, c := range crunchers {
		c.Done <- c.GenerateCruncherFileName(f) // TODO ovo je redudantno kad zavrim nove izmene
	}
	fmt.Println("FileInput => ReadFile => finished readings", f.File.Name())
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
	splitWords := createTextChunks(s.Text, app.CounterLimit)
	cc := cr.GetOrCreateCounter(s.FileName)
	for _, d := range splitWords {
		go cc.CountSegment(s.FileName, d)
	}
	cc.WriteResults()
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

func (cc *CruncherCounter) Listen() {
	for {
		m := <-cc.Done
		mergeMaps(cc.Data, m)
		cc.WaitGroup.Done()
	}
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
	fmt.Printf("CruncherCounter => CountSegment => finished counting segment with word length %d for %s\n", len(m), fn)
	cc.Done <- m
}

func (cc *CruncherCounter) WriteResults() {
	fmt.Printf("CruncherCounter => WriteResults => waiting to finish all jobs for %s\n", cc.FileName)
	cc.WaitGroup.Wait()
	fmt.Printf("CruncherCounter => WriteResults => finished waiting for %s\n", cc.FileName)
	fmt.Printf("CruncherCounter => WriteResults => started streaming to output for %s\n", cc.FileName)
	o.Stream <- OutputStream{cc.FileName, cc.Data}
	cc.ResetData()
}

func (cc *CruncherCounter) ResetData() {
	cc.Data = make(map[string]int)
}

type Output struct {
	Stream     chan OutputStream
	Done       chan *OutputFile
	OutputFile []*OutputFile
}

type OutputStream struct {
	FileName string
	Data     map[string]int
}

func NewOutput() *Output {
	o := &Output{
		Stream: make(chan OutputStream),
		Done:   make(chan *OutputFile),
	}
	go o.Listen()
	return o
}

func (o *Output) GetOutputFile(fn string) *OutputFile {
	for _, of := range o.OutputFile {
		if of.FileName == fn {
			return of
		}
	}
	return nil
}

func (o *Output) GetOrCreateOutputFile(fn string) *OutputFile {
	of := o.GetOutputFile(fn)
	if of == nil {
		of = NewOutputFile(fn)
		o.OutputFile = append(o.OutputFile, of)
	}
	return of
}

func (o *Output) Listen() {
	for {
		select {
		case s := <-o.Stream:
			of := o.GetOrCreateOutputFile(s.FileName)
			of.Data = s.Data
			go of.WriteToFile()
			break
		}
	}
}

func (o *Output) MergeFiles(fn string) {
	ofs := []*OutputFile{}
	paths := strings.Split(fn, ",")
	for _, p := range paths {
		t := o.GetOutputFile(p)
		if t == nil {
			fmt.Println("Output => MergeFiles => file not found")
			return
		}
		ofs = append(ofs, t)
	}

	defer func() {
		for _, of := range ofs {
			of.Mutex.Unlock()
		}
	}()

	f := "m"
	for _, of := range ofs {
		of.Mutex.Lock()
		f = fmt.Sprintf("%s-%s", f, of.FileName)
	}

	of := o.GetOrCreateOutputFile(f)
	for _, o := range ofs {
		of.Data = mergeMaps(of.Data, o.Data)
	}
	of.WriteToFile()
}

type OutputFile struct {
	FileName string
	Data     map[string]int
	Mutex    sync.Mutex
}

func NewOutputFile(fn string) *OutputFile {
	return &OutputFile{
		FileName: fn,
		Data:     make(map[string]int),
	}
}

func (of *OutputFile) WriteToFile() {
	path := fmt.Sprintf("./output/%s", of.FileName)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Outpuit => WriteToFile => error while writing ", r)
		}
		fmt.Printf("OutputFile => WriteToFile => finished writing %s\n", path)
		of.Mutex.Unlock()
	}()

	of.Mutex.Lock()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("failed creating or opening file: %s\n", err)
		return
	}

	defer f.Close()

	sorted := mapToKVSorted(of.Data)

	for _, s := range sorted {
		if _, err = f.WriteString(fmt.Sprintf("%s %d\n", s.Key, s.Value)); err != nil {
			fmt.Printf("failed to write to file: %s\n", err)
			return
		}
	}

	if app.SlowWrite {
		time.Sleep(time.Second * 30)
	}
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

func readFile(path string) (string, error) {
	fmt.Println("FileInput => ReadFile => opening file", path)

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("FileInput => Read File => failed reading file %s: %s\n", path, err)
		return "", err
	}

	text := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text += scanner.Text()
	}

	if err = file.Close(); err != nil {
		fmt.Printf("FileInput => Read File => failed closing file %s: %s\n", path, err)
		return text, err
	}

	return text, nil
}

type KeyValue struct {
	Key   string
	Value int
}

func mapToKVSorted(m map[string]int) []*KeyValue {
	var ss []*KeyValue
	for k, v := range m {
		ss = append(ss, &KeyValue{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})
	return ss
}

func createTextChunks(text string, chunk int) [][]string {
	results := [][]string{}
	words := strings.Fields(text)
	counter := chunk
	curr := []string{}
	for _, w := range words {
		curr = append(curr, w)
		counter -= len(w)
		if counter < 0 {
			t := curr
			results = append(results, t)
			counter = chunk
			curr = []string{}
		}
	}
	if len(curr) > 0 {
		results = append(results, curr)
	}
	return results
}
