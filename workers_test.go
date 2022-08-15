package workers

import (
	"fmt"
	"testing"
	"time"
)

var workers = New(10)

type tester struct {
	name string
	n    int
}

//单实例
func TestWorkers(t *testing.T) {
	type copyAt struct {
		n int
	}
	for i := 1; i <= 100; i++ {
		c := copyAt{n: i}
		//利用 Submit 提交一个等候执行的方法
		workers.Submit(func() {
			time.Sleep(time.Second * 2)
			fmt.Println(i, c.n)
		})
	}
	workers.Wait()
	for {
		time.Sleep(time.Second)
		fmt.Println("==================", workers.JobTotal, "==================")
	}
}

//任务终止
func TestWorkersStop(t *testing.T) {
	for i := 0; i < 100; i++ {
		workers.Add(tester{n: i})
	}
	fmt.Println("等待1")
	timeout := workers.Wait(time.Second * 3)
	if timeout {
		workers.Stop()
	}
	if timeout {
		fmt.Println("结果超时")
	} else {
		fmt.Println("结果完成")
	}
	fmt.Println("等待2")
	timeout = workers.Wait(time.Second * 3)
	if timeout {
		fmt.Println("结果超时")
	} else {
		fmt.Println("结果完成")
	}
	fmt.Println("等待3")
	timeout = workers.Wait(time.Second * 3)
	if timeout {
		fmt.Println("结果超时")
	} else {
		fmt.Println("结果完成")
	}
	time.Sleep(time.Second * 600)
}

//任务终止后继续
func TestWorkersStopNext(t *testing.T) {
	for i := 0; i < 100; i++ {
		workers.Add(tester{n: i})
	}
	if workers.Wait(time.Second * 3) {
		//超时后停止
		workers.Stop()
		fmt.Println("超时,终止任务")
	}
	fmt.Println("继续任务")
	//继续
	for i := 100; i < 150; i++ {
		workers.Add(tester{n: i})
	}
	fmt.Println("next wait:", workers.Wait())
}

//多实例等待
func TestMultipleWorkers(t *testing.T) {
	workers1 := New(5)
	workers2 := New(5)
	//超时时间
	//workers1.Timeout = time.Second * 1
	//workers2.Timeout = time.Second * 3
	for i := 0; i < 50; i++ {
		workers1.Add(tester{n: i, name: "A"})
	}
	for i := 0; i < 50; i++ {
		workers2.Add(tester{n: i, name: "B"})
	}
	timeout := WaitAll(workers1, workers2)

	fmt.Println(timeout)
	time.Sleep(time.Second * 600)
}

func (t tester) ToWork() error {
	time.Sleep(time.Second * 2)
	fmt.Println(t.n, t.name)
	return nil
}

const (
	BenchParam         = 10
	BenchAntsSize      = 200000
	DefaultExpiredTime = 10 * time.Second
)

//cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
//BenchmarkWorkers
//BenchmarkWorkers-8   	14158902	        72.60 ns/op
func BenchmarkWorkers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		workers.Submit(func() {
			demoFunc()
		})
	}
}

//
//var antsPool, _ = ants.NewPool(BenchAntsSize, ants.WithExpiryDuration(DefaultExpiredTime))
//
////cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
////BenchmarkAnts
////BenchmarkAnts-8   	 1448713	       786.7 ns/op
//func BenchmarkAnts(b *testing.B) {
//	for i := 0; i < b.N; i++ {
//		antsPool.Submit(func() {
//			demoFunc()
//		})
//	}
//}

func demoFunc() {
	time.Sleep(time.Duration(BenchParam) * time.Millisecond)
}
