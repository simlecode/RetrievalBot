package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/data-preservation-programs/RetrievalBot/pkg/task"
	"github.com/data-preservation-programs/RetrievalBot/worker/http"
)

type Provider struct {
	ID         string
	PeerID     string
	Multiaddrs []string
	Pieces     []string
}

func main() {
	var cfg Provider

	cfgPath := new(string)
	v := new(bool)
	flag.StringVar(cfgPath, "config", "./http_retrieval.toml", "http retrieval config")
	flag.BoolVar(v, "v", false, "")
	flag.Parse()
	// fmt.Println("config path:", *cfgPath)

	f, err := os.Open(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := toml.NewDecoder(f).Decode(&cfg); err != nil {
		log.Fatal(err)
	}
	// fmt.Printf("config: %+v\n", cfg)

	tsk := task.Task{
		Timeout: time.Minute * 5,
		Provider: task.Provider{
			ID:         cfg.ID,
			PeerID:     cfg.PeerID,
			Multiaddrs: cfg.Multiaddrs,
		},
	}

	worker := http.Worker{}
	for _, piece := range cfg.Pieces {
		tsk.Content.CID = piece
		res, err := worker.DoWork(tsk)
		if err != nil {
			fmt.Printf("retrieval %s failed: %s\n", piece, err)
		} else if res.Success {
			fmt.Printf("retrieval %s success\n", piece)
			if *v {
				fmt.Printf("response %+v", res)
			}
		} else {
			fmt.Printf("response failed: %+v\n", res)
		}
	}

	// process, err := task.NewTaskWorkerProcess(context.Background(), task.HTTP, worker)
	// if err != nil {
	//  panic(err)
	// }

	// defer process.Close()

	// err = process.Poll(context.Background())
	// if err != nil {
	//  logging.Logger("task-worker").With("protocol", task.HTTP).Error(err)
	// }
}
