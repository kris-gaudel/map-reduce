package main

import (
	"flag"
	"fmt"
	"log"
	mr "mapreduce/lib"
	"os"
	"path/filepath"
	"plugin"
	"time"
)

var (
	mode         = flag.String("mode", "master", "Specify 'master' or 'worker' mode")
	masterFiles  = flag.String("masterFiles", "", "Files for master mode")
	workerCount  = flag.Int("workerCount", 1, "Number of workers for worker mode")
	workerPlugin = flag.String("workerPlugin", "", "Path to worker plugin for worker mode")
)

func main() {
	flag.Parse()
	switch *mode {
	case "master":
		if *masterFiles == "" {
			fmt.Println("Please specify files for master mode")
			os.Exit(1)
		}
		runMaster(*masterFiles)
	case "worker":
		if *workerPlugin == "" {
			fmt.Println("Please specify a worker plugin")
			os.Exit(1)
		}
		runWorkers(*workerCount, *workerPlugin)
	default:
		fmt.Println("Invalid mode")
		os.Exit(1)
	}
}

func runWorkers(workerCount int, workerPlugin string) {
	fmt.Printf("Starting %d worker nodes!\n", workerCount)
	fmt.Printf("Using plugin: %s\n", workerPlugin)

	mapf, reducef := loadPlugin(workerPlugin)
	fmt.Printf("Worker loaded plugin successfully!\n")

	for i := 0; i < workerCount; i++ {
		// Start worker
		mr.Worker(mapf, reducef)
	}
}

func runMaster(masterFiles string) {
	fmt.Println("Starting master node!")
	files, err := listFilesInFolder(masterFiles)
	if err != nil {
		log.Fatalf("Error listing files in folder: %v", err)
	}
	// Print files
	fmt.Println("Files:")
	for _, file := range files {
		fmt.Println(file)
	}
	master := mr.MakeMaster(files, 1)
	for !master.Done() {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}

func listFilesInFolder(folderPath string) ([]string, error) {
	var files []string

	// Open the directory
	f, err := os.Open(folderPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read the directory entries
	fileInfo, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Collect the file names
	for _, file := range fileInfo {
		if !file.IsDir() { // Skip directories
			fullPath := filepath.Join(folderPath, file.Name())
			files = append(files, fullPath)
		}
	}

	return files, nil
}

func loadPlugin(pluginPath string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// Load plugin
	p, err := plugin.Open(pluginPath)
	if err != nil {
		log.Fatalf("Error opening plugin: %v", err)
	}

	// Extract symbols from plugin
	mapSymbol, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("Error finding Map function symbol: %v", err)
	}
	reduceSymbol, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("Error finding Reduce function symbol: %v", err)
	}

	// Convert symbols to respective functions
	mapFunc, ok := interface{}(mapSymbol).(func(string, string) []mr.KeyValue)
	if !ok {
		log.Fatalf("Error casting Map function symbol")
	}
	reduceFunc, ok := interface{}(reduceSymbol).(func(string, []string) string)
	if !ok {
		log.Fatalf("Error casting Reduce function symbol")
	}

	return mapFunc, reduceFunc
}
