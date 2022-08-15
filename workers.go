/*
 * Copyright (c) 2021 BlueStorm
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFINGEMENT IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package workers

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"workers/queue"
)

type Job interface {
	ToWork() error
}

type worker struct {
	task chan Job
}

type Dispatcher struct {
	MaxWorkers int32             //最大工人数
	JobQueue   *queue.Queue[Job] //工作队列
	JobTotal   int32             //工作数量
	Timeout    time.Duration     //超时时间
	pool       sync.Pool         //sync.Pool
	wait       chan bool
	locker     sync.Mutex
	quit       chan bool
	complete   chan bool
	started    bool
	stop       bool
	running    int32 //执行中
	ready      bool
	lock       sync.Mutex
	mu         sync.Mutex
	alive      int32
}

// Default 获取一个默认配置的工作者对象，取得最佳效果
func Default() *Dispatcher {
	return NewWorkerQueue(1000, 30000) //math.MaxInt32
}

// New 声明一个工作者，队列自动扩容
func New(worker int32) *Dispatcher {
	d := NewWorkerQueue(1000, worker)
	return d
}

// NewWorkerQueue 获得工人工作队列的调度器并直接启动
func NewWorkerQueue(maxQueue, maxWorker int32) *Dispatcher {
	dispatcher := newDispatcher(maxWorker)
	dispatcher.JobQueue = queue.New[Job](int(maxQueue))
	dispatcher.wait = make(chan bool, 1)
	dispatcher.pool.New = func() any {
		return &worker{}
	}
	return dispatcher
}

//生成调度器
func newDispatcher(maxWorkers int32) *Dispatcher {
	return &Dispatcher{MaxWorkers: maxWorkers}
}

func (d *Dispatcher) WorkerTotal() int32 {
	return d.MaxWorkers
}

// Add 添加工作
func (d *Dispatcher) Add(job Job) (check bool) {
	if check = d.JobQueue.Push(job); check {
		d.incTotal()
		d.Run()
	}
	return
}

// Submit 提交一个等候执行的方法
func (d *Dispatcher) Submit(f func()) {
	d.Add(event{f: f})
}

// Wait 等待结果完成 timeout 超时时间
func (d *Dispatcher) Wait(timeout ...time.Duration) bool {
	if d.Total() == 0 {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.complete == nil {
		d.complete = make(chan bool, 1)
	} else {
		select {
		case <-d.complete:
		default:
		}
	}
	if len(timeout) > 0 && timeout[0] > 0 {
		go func() {
			if _, ok := <-time.After(timeout[0]); ok {
				d.complete <- true
			}
		}()
	}
	if status, ok := <-d.complete; ok {
		return status
	}
	return false
}

// WaitAll 等待所有工作者完成
func WaitAll(workers ...*Dispatcher) bool {
	total := len(workers)
	if total == 0 {
		return false
	}
	var wg sync.WaitGroup
	wg.Add(total)
	var dispatchers []*Dispatcher
	for _, d := range workers {
		go func(dispatcher *Dispatcher) {
			if dispatcher.Wait(dispatcher.Timeout) {
				dispatchers = append(dispatchers, dispatcher)
			}
			wg.Done()
		}(d)
	}
	wg.Wait()
	for _, d := range dispatchers {
		d.Stop()
	}
	return len(dispatchers) > 0
}

// Run 启动调度器
func (d *Dispatcher) Run() *Dispatcher {
	if !d.started {
		d.lock.Lock()
		if !d.started {
			d.started = true
			//调度器开始工作
			go d.dispatch()
		}
		d.lock.Unlock()
	}
	return d
}

// Stop 停止所有工作
func (d *Dispatcher) Stop() *Dispatcher {
	if d.JobTotal == 0 {
		return d
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.quit == nil {
		d.quit = make(chan bool, 1)
	}
	d.stop = true
	<-d.quit
	d.stop = false
	return d
}

// Running 获取当前执行数量
func (d *Dispatcher) Running() int32 {
	return atomic.LoadInt32(&d.running)
}

// Total 获取当前任务总数
func (d *Dispatcher) Total() int32 {
	return atomic.LoadInt32(&d.JobTotal)
}

// Alive 获取当前工作工人数量
func (d *Dispatcher) Alive() int32 {
	return atomic.LoadInt32(&d.alive)
}

//调度
func (d *Dispatcher) dispatch() {
	defer func() {
		d.started = false
	}()
	for {
		//获取工作
		job := d.JobQueue.Pop()
		//没有工作内容，停止工作调度
		if job == nil {
			return
		}
		if d.Running() >= d.MaxWorkers {
			d.ready = true
			<-d.wait
		}
		d.incRunning()
		w := d.pool.Get().(*worker)
		if w.task == nil {
			d.run(w)
		}
		w.task <- job
	}
}

func (d *Dispatcher) run(w *worker) {
	w.task = make(chan Job, 1)
	d.incAlive()
	go func() {
		defer func() {
			w.task = nil
			d.pool.Put(w)
			d.decRunning()
			d.decAlive()
			if r := recover(); r != nil {
				log.Println("work exception", r)
			}
		}()
		for t := range w.task {
			if !d.stop {
				if err := t.ToWork(); err != nil {
					log.Println("work error", err)
				}
			}
			//完成工作
			d.pool.Put(w)
			//准备
			if d.decRunning() < d.MaxWorkers {
				if d.ready {
					d.locker.Lock()
					if d.ready {
						d.ready = false
						d.wait <- true
					}
					d.locker.Unlock()
				}
			}
			if d.decTotal() == 0 {
				if d.stop {
					d.quit <- true
				} else if d.complete != nil {
					d.complete <- false
				}
			}
		}
	}()
}

func (d *Dispatcher) incRunning() int32 {
	return atomic.AddInt32(&d.running, 1)
}

func (d *Dispatcher) decRunning() int32 {
	return atomic.AddInt32(&d.running, -1)
}

func (d *Dispatcher) incTotal() int32 {
	return atomic.AddInt32(&d.JobTotal, 1)
}

func (d *Dispatcher) decTotal() int32 {
	return atomic.AddInt32(&d.JobTotal, -1)
}

//func (d *Dispatcher) incLocker() {
//	atomic.StoreInt32(&d.locker, 1)
//}
//
//func (d *Dispatcher) decLocker() {
//	atomic.StoreInt32(&d.locker, 0)
//}

func (d *Dispatcher) incAlive() int32 {
	return atomic.AddInt32(&d.alive, 1)
}

func (d *Dispatcher) decAlive() int32 {
	return atomic.AddInt32(&d.alive, -1)
}

type event struct {
	f func()
}

func (e event) ToWork() error {
	e.f()
	return nil
}
