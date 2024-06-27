package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

type Stats struct {
	Min   float64
	Max   float64
	Mean  float64
	Count int64
}

func (s *Stats) AddRecord(temp float64) {
	if temp < s.Min {
		s.Min = temp
	}
	if temp > s.Max {
		s.Max = temp
	}
	s.Mean = (s.Mean*float64(s.Count) + temp) / (float64(s.Count) + 1)
	s.Count++
}

func NewStats() Stats {
	return Stats{
		Min:   math.MaxFloat64,
		Max:   -1 * math.MaxFloat64,
		Mean:  0,
		Count: 0,
	}
}

func parseLine(line string) (string, float64, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("Line does not follow format <city>;<temp>. Received line %q", line)
	}
	city := parts[0]
	temp, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return "", 0, fmt.Errorf("Failed to parse temperature. Received %q", parts[1])
	}
	return city, temp, nil
}

func main() {
	fh, err := os.Open("test.txt")
	if err != nil {
		log.Fatalf("Error: %+v", err)
	}
	scanner := bufio.NewScanner(fh)

	m := make(map[string]*Stats)

	var numlines int
	for scanner.Scan() {
		numlines++
		line := scanner.Text()
		city, temp, err := parseLine(line)
		if err != nil {
			log.Fatalf("Failed to parse line %q", line)
		}
		entry, ok := m[city]
		if !ok {
			m[city] = &Stats{
				Min:   temp,
				Max:   temp,
				Mean:  temp,
				Count: 1,
			}
		} else {
			entry.AddRecord(temp)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error: %+v", err)
	}
	fmt.Printf("Found %d lines\n", numlines)
	for key, val := range m {
		fmt.Printf("City %s: \n", key)
		fmt.Printf("\t%+v\n", *val)
	}
}
