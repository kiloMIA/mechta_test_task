package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type Data struct {
	A int `json:"a"`
	B int `json:"b"`
}

func worker(data []Data, ch chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	sum := 0
	for _, item := range data {
		sum += item.A + item.B
	}
	ch <- sum
}

func readFile(fileLocation string) ([]Data, error) {
	byteValue, err := os.ReadFile(fileLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", fileLocation, err)
	}

	var data []Data
	if err := json.Unmarshal(byteValue, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	return data, nil
}

func processChunks(data []Data, numWorkers int) int {
	dataLen := len(data)
	if dataLen == 0 {
		return 0
	}

	chunkSize := (dataLen + numWorkers - 1) / numWorkers
	ch := make(chan int, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= dataLen {
			break
		}
		if end > dataLen {
			end = dataLen
		}
		wg.Add(1)
		go worker(data[start:end], ch, &wg)
	}

	wg.Wait()
	close(ch)

	totalSum := 0
	for sum := range ch {
		totalSum += sum
	}

	return totalSum
}

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: %s <file_location> <number_of_workers>", os.Args[0])
	}

	fileLocation := os.Args[1]
	numWorkers, err := strconv.Atoi(os.Args[2])
	if err != nil || numWorkers <= 0 {
		log.Fatalf("Invalid number of workers: %v", os.Args[2])
	}

	data, err := readFile(fileLocation)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	start := time.Now()
	totalSum := processChunks(data, numWorkers)
	elapsed := time.Since(start)

	fmt.Printf("Total Sum: %d\n", totalSum)
	fmt.Printf("Time taken with %d workers: %s\n", numWorkers, elapsed)
}
