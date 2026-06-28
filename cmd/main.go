package main

import (
	"context"
	"log"

	"github.com/dawsonalex/iotflow"
)

func main() {
	p, err := iotflow.NewNetworkManagerProvisioner("wlan0")
	if err != nil {
		log.Fatal(err)
	}
	defer func(p *iotflow.NetworkManagerProvisioner) {
		err := p.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(p)

	f, err := iotflow.NewFlow(p)
	if err != nil {
		log.Fatal(err)
	}

	defer func(f *iotflow.Flow) {
		err := f.Finish()
		if err != nil {
			log.Fatal(err)
		}
	}(f)

	err = f.Begin(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	for upd := range f.Subscribe() {
		if upd.Err != nil {
			log.Fatal(upd.Err)
		}
		log.Printf("state: %s", upd.State)
	}
}
