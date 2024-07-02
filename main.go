package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
)

const numWorkers = 10

type Stats struct {
	Min   float32
	Max   float32
	Sum   float32
	Count int
}

func (s *Stats) AddRecord(temp float32) {
	if temp < s.Min {
		s.Min = temp
	}
	if temp > s.Max {
		s.Max = temp
	}
	s.Sum += temp
	s.Count++
}

func NewStats() Stats {
	return Stats{
		Min:   math.MaxFloat32,
		Max:   -1 * math.MaxFloat32,
		Sum:   0,
		Count: 0,
	}
}

type TempRecord struct {
	Name string
	Temp float32
}

func parseLine(line string) (string, float32, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("Line does not follow format <city>;<temp>. Received line %q", line)
	}
	city := parts[0]
	temp, err := strconv.ParseFloat(parts[1], 32)
	if err != nil {
		return "", 0, fmt.Errorf("Failed to parse temperature. Received %q", parts[1])
	}
	return city, float32(temp), nil
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

func brc(processWG *sync.WaitGroup) {
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

func main() {
	var wg sync.WaitGroup
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	wg.Add(1)
	go brc(&wg)
	wg.Wait()
}

// func main() {
// 	brc()
// }
