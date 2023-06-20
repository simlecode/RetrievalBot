package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/data-preservation-programs/RetrievalBot/pkg/task"
	"github.com/data-preservation-programs/RetrievalBot/worker/http"
)

type Cfg struct {
	Common struct {
		PeerID     string
		Multiaddrs []string
		Pieces     []string
	}
	Providers []Provider
}

type Provider struct {
	ID         string
	PeerID     string
	Multiaddrs []string
	Pieces     []string
}

func main() {
	cfgPath := new(string)
	v := new(bool)
	flag.StringVar(cfgPath, "config", "./http_retrieval.toml", "http retrieval config")
	flag.BoolVar(v, "v", false, "")
	flag.Parse()
	// fmt.Println("config path:", *cfgPath)

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("config: %+v\n\n", cfg)

	worker := http.Worker{}
	tasks := toTasks(cfg)
	fmt.Printf("has %d tasks\n", len(tasks))
	for _, tsk := range tasks {
		res, err := worker.DoWork(*tsk)
		if err != nil {
			fmt.Printf("miner %s retrieval %s failed: %s\n", tsk.Provider.ID, tsk.Content.CID, err)
		} else if res.Success {
			fmt.Printf("miner %s retrieval %s success\n", tsk.Provider.ID, tsk.Content.CID)
			if *v {
				fmt.Printf("miner %s retrieval %s success, response: %+v\n", tsk.Provider.ID, tsk.Content.CID, res)
			}
		} else {
			fmt.Printf("miner %s retrieval %s failed, response: %+v\n", tsk.Provider.ID, tsk.Content.CID, res)
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

func loadConfig(cfgPath string) (*Cfg, error) {
	oldCfg := Provider{}
	cfg := Cfg{}

	f, err := os.Open(cfgPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	_, err = toml.NewDecoder(bytes.NewReader(data)).Decode(&oldCfg)
	if err == nil {
		if len(oldCfg.ID) != 0 {
			cfg.Providers = append(cfg.Providers, oldCfg)

			return &cfg, nil
		}
	}

	_, err = toml.NewDecoder(bytes.NewReader(data)).Decode(&cfg)

	return &cfg, err
}

func toTasks(cfg *Cfg) []*task.Task {
	var tasks []*task.Task
	commonPieces := cfg.Common.Pieces
	commonPeerID := cfg.Common.PeerID
	commonMultiaddrs := cfg.Common.Multiaddrs
	for _, p := range cfg.Providers {
		t := task.Task{
			Timeout: time.Minute * 5,
			Provider: task.Provider{
				ID:         p.ID,
				PeerID:     p.PeerID,
				Multiaddrs: p.Multiaddrs,
			},
		}
		if len(p.ID) == 0 {
			continue
		}
		if len(p.PeerID) == 0 && len(commonPeerID) != 0 {
			t.Provider.PeerID = commonPeerID
		}
		if len(p.Multiaddrs) == 0 && len(commonMultiaddrs) == 0 {
			t.Provider.Multiaddrs = commonMultiaddrs
		}
		pieces := p.Pieces
		if len(pieces) == 0 {
			if len(commonPieces) == 0 {
				continue
			}
			pieces = commonPieces
		}

		for _, piece := range pieces {
			if len(piece) == 0 {
				continue
			}
			tmp := t
			tmp.Content.CID = piece
			tasks = append(tasks, &tmp)
		}
	}

	return tasks
}
