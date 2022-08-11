# workers
高性能多任务工作者队列，队列支持自动扩容

也可作用于 goroutine 池，是 ants 的10倍

``` golang
var workers = New(10000)

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

const (
	BenchParam         = 10
	BenchAntsSize      = 200000
	DefaultExpiredTime = 10 * time.Second
)

var antsPool, _ = ants.NewPool(BenchAntsSize, ants.WithExpiryDuration(DefaultExpiredTime))

//cpu: Intel(R) Core(TM) i7-1068NG7 CPU @ 2.30GHz
//BenchmarkAnts
//BenchmarkAnts-8   	 1448713	       786.7 ns/op
func BenchmarkAnts(b *testing.B) {
	for i := 0; i < b.N; i++ {
		antsPool.Submit(func() {
			demoFunc()
		})
	}
}

func demoFunc() {
	time.Sleep(time.Duration(BenchParam) * time.Millisecond)
}
```