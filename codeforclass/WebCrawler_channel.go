package codeforclass

import "fmt"

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

func master(fetcher Fetcher, ch chan []string){
	n := 1
	fetched := make(map[string]bool)
	for urls := range ch{
		n -= 1
		for _, url := range urls{
			if fetched[url] == false {
				fetched[url] = true
				 n += 1
				go worker(url, fetcher, ch)
			}
		}
		if n == 0 {
			break
		}
	}
}
func worker(url string, fetcher Fetcher, ch chan []string){
	_, urls, err := fetcher.Fetch(url)
	if err == nil {
		ch<-urls
	}else {
		ch<-[]string{}	
	}
}
func concurrentChannel(url string, fetcher Fetcher){
	ch := make(chan []string)
	go func(){ch<-[]string{url}}()   // 不用goroutine会死锁，因为cap为0的channel在接收方未准备的情况下会阻塞发送方
	master(fetcher, ch)
}
func main() {
	concurrentChannel("https://golang.org/", fetcher)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
