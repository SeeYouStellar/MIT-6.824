package main

import(
	"fmt"
	"time"
)

func thread(num int) {
	if num == 3 {
		time.Sleep(3*time.Second)
		panic("thread 3 dead")
	}
	
	for {
		fmt.Printf("thread %d is running\n", num)
	}
}

func main(){
	for i:=0;i<5;i++ {
		go thread(i)
	}
	for {
		fmt.Println("main thread is running")
	}
}