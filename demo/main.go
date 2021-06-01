package main

import (
	"fmt"
	"github.com/japier/gkpcs"
	"github.com/japier/gkpcs/demo/example"
)

func main() {
	fmt.Println("hola")
	sm := &example.SimpleMessage{
		Id:   1,
		Text: "Hello",
	}
	dataBytes, _ := gkpcs.Serialize(sm)
	fmt.Println(dataBytes)

	sm2 := &example.SimpleMessage{}

	if err := gkpcs.Deserialize(dataBytes, sm2); err != nil {
		fmt.Println(err)
	}
	fmt.Println(sm2.Text)
}
