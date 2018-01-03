package main

import (
	"log"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/mindy"
)

func main() {
	err := cobrafy.Execute(mindy.NewMain())
	if err != nil {
		log.Fatalf("executing mindy: %v", err)
	}
}
