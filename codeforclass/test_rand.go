package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	fmt.Printf("use diff seed--------------\n")
	for i := 0; i < 5; i++ {
		rand.Seed(time.Now().UnixNano())
		num := rand.Intn(10)
		fmt.Printf("%d ", num)
	}
	// 5 7 2 0 4
	// 6 2 7 5 2
	// 4 4 0 0 7
	// get the different output when run the file

	// fmt.Printf("\n")
	// fmt.Printf("use same seed--------------\n")
	// for i := 0; i < 5; i++ {
	// 	num := rand.Intn(10)
	// 	fmt.Printf("%d ", num)
	// }
	// 1 7 7 9 1
	// 1 7 7 9 1
	// 1 7 7 9 1
	// get the same output when run the file

	fmt.Printf("\n")

}
