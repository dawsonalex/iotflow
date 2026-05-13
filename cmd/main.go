package main

import (
	"context"
	"log"

	"github.com/dawsonalex/iotflow"
)

func main() {
	p, err := iotflow.NewNmProvisioner(iotflow.WithInterface("wlp2s0"))
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	updates, err := p.ConnectToNetwork(context.Background(), "SKYV5S1C", "vmetHk8ef6MyYe")
	if err != nil {
		log.Fatal(err)
	}

	for upd := range updates {
		if upd.Err != nil {
			log.Fatal(upd.Err)
		}
		log.Printf("state: %s", upd.State)
	}
}
