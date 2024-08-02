package brc

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

const numWorkers = 10

type TempRecord struct {
	Name string
	Temp float32
}

func parseWorker(lineChan chan string, recordChan chan TempRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		line, more := <-lineChan
		if !more {
			break
		}
		city, temp, err := parseLine(line)
		if err != nil {
			log.Fatalf("Failed to parse line: %v", err)
		}
		recordChan <- TempRecord{city, temp}
	}
}

func mapBuilder(m map[string]*Stats, c chan TempRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		t, more := <-c
		if !more {
			break
		}
		entry, ok := m[t.Name]
		if !ok {
			m[t.Name] = &Stats{
				Min:   t.Temp,
				Max:   t.Temp,
				Sum:   t.Temp,
				Count: 1,
			}
		} else {
			entry.AddRecord(t.Temp)
		}
	}
}

func BRCAsync(processWG *sync.WaitGroup) {
	defer processWG.Done()
	fh, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatalf("Error: %+v", err)
	}
	scanner := bufio.NewScanner(fh)

	m := make(map[string]*Stats)

	var numlines int

	var collectorWG sync.WaitGroup
	collectorWG.Add(1)
	recordChan := make(chan TempRecord, 1000)
	go mapBuilder(m, recordChan, &collectorWG)

	var parserWG sync.WaitGroup
	lineChan := make(chan string, 500)
	for range numWorkers {
		parserWG.Add(1)
		go parseWorker(lineChan, recordChan, &parserWG)
	}

	for scanner.Scan() {
		line := scanner.Text()
		lineChan <- line
	}

	close(lineChan)
	parserWG.Wait()
	close(recordChan)
	collectorWG.Wait()

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error: %+v", err)
	}

	fmt.Printf("Found %d lines\n", numlines)
	for key, val := range m {
		fmt.Printf("City %s: \n", key)
		fmt.Printf("\t%+v\n", *val)
	}
}
