package main

import (
	"fmt"
	"github.com/socialsalt/brc/internal"
	"net/http"
	_ "net/http/pprof"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	wg.Add(1)
	go brc.BRCAsync(&wg)
	wg.Wait()

}
