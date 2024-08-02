package brc

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

func BRC() {
	fh, err := os.Open("measurements.txt")
	if err != nil {
		log.Fatalf("Error, failed to open file: %s", err)
	}

	scanner := bufio.NewScanner(fh)

	m := make(map[string]*Stats)
	var numlines uint32

	for scanner.Scan() {
		line := scanner.Text()
		city, temp, err := parseLine(line)
		if err != nil {
			log.Fatalf("Failed to parse line: %s\nWith error: %s", line, err)
		}
		entry, ok := m[city]
		if !ok {
			m[city] = &Stats{
				Min:   temp,
				Max:   temp,
				Sum:   temp,
				Count: 1,
			}
		} else {
			entry.AddRecord(temp)
		}
		numlines++
	}

	fmt.Printf("Found %d lines\n", numlines)
	for key, val := range m {
		fmt.Printf("City %s: \n", key)
		fmt.Printf("\t%+v\n", *val)
	}
}
