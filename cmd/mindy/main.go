package main

import (
	"log"

	"github.com/jaffee/commandeer/cobradeer"
	"github.com/pilosa/mindy"
)

func main() {

	m := mindy.NewMain()
	com, err := cobradeer.Cobra(m)
	if err != nil {
		log.Fatalf("creating command: %v", err)
	}
	err = com.Execute()
	if err != nil {
		log.Fatalf("executing command: %v", err)
	}

}
