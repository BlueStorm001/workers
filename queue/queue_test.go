package queue

import (
	"fmt"
	"testing"
	"time"
)

type abc struct {
	n int
	s string
}

var rq = New[*abc]()

func TestQueue(t *testing.T) {
	for i := 0; i < 6; i++ {
		rq.Push(&abc{n: i})
	}
	for {
		v := rq.FILO()
		if v == nil {
			break
		}
		fmt.Println("FILO", v.n)
	}
	for i := 5; i < 10; i++ {
		rq.Push(&abc{n: i})
	}
	for {
		v := rq.FILO()
		if v == nil {
			break
		}
		fmt.Println("FILO", v.n)
	}
}

func TestQueueNS(t *testing.T) {
	var n int
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			n++
			rq.Push(&abc{n: n})
			//fmt.Println("Push", n)
			if n >= 15 {
				break
			}
		}
	}()
	go func() {
		for {
			time.Sleep(time.Second)
			v := rq.Pop()
			if v == nil {
				fmt.Println("Pop", "null")
			} else {
				fmt.Println("Pop", v.n)
			}
		}
	}()
	time.Sleep(time.Hour)
}

func TestQueueT(t *testing.T) {
	for i := 0; i < 95; i++ {
		rq.Push(&abc{n: i})
	}
	for i := 0; i < 5; i++ {
		v := rq.Pop()
		if v == nil {
			break
		}
	}
	for i := 95; i < 100; i++ {
		rq.Push(&abc{n: i})
	}
	for {
		v := rq.Pop()
		if v == nil {
			break
		}
		fmt.Println("Pop", v.n)
	}
	fmt.Println("Pop", rq)
}

func TestBenchmark(t *testing.T) {
	var total = 100000000
	now := time.Now()
	for i := 1; i <= total; i++ {
		rq.Push(&abc{n: i})
	}
	fmt.Println("Push", time.Since(now))
	now = time.Now()
	for {
		r := rq.Pop()
		if r == nil {
			break
		}
	}
	fmt.Println("Pop", time.Since(now))
	fmt.Println(total)
}

func BenchmarkQueue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rq.Push(&abc{n: i})
		rq.Pop()
	}
}

func TestBenchmark1(t *testing.T) {
	for {
		time.Sleep(time.Millisecond)
	}
}
