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
	"fmt"
	"sync"
	"time"
	"workers/queue"
)

type Job interface {
	ToWork() error
}

type Dispatcher struct {
	MaxWorkers int               //最大工人数
	JobQueue   *queue.Queue[Job] //工作队列
	JobTotal   int               //工作数量
	Timeout    time.Duration     //超时时间
	tally      sync.Mutex
	quit       chan bool
	complete   chan bool
	stop       bool
	mu         sync.Mutex
	run        bool
	rm         sync.Mutex
	executions int //执行中
}

// New 声明一个工作者，队列自动扩容
func New(worker int) *Dispatcher {
	d := NewWorkerQueue(1000, worker)
	return d
}

// NewWorkerQueue 获得工人工作队列的调度器并直接启动
func NewWorkerQueue(maxQueue, maxWorker int) *Dispatcher {
	dispatcher := newDispatcher(maxWorker)
	dispatcher.JobQueue = queue.New[Job](maxQueue)
	return dispatcher
}

//生成调度器
func newDispatcher(maxWorkers int) *Dispatcher {
	return &Dispatcher{MaxWorkers: maxWorkers}
}

func (d *Dispatcher) WorkerTotal() int {
	return d.MaxWorkers
}

// Add 添加工作
func (d *Dispatcher) Add(job Job) (check bool) {
	if check = d.JobQueue.Push(job); check {
		d.tally.Lock()
		d.JobTotal++
		d.tally.Unlock()
		d.Run()
	}
	return
}

// Submit 提交一个等候执行的方法
func (d *Dispatcher) Submit(f func()) {
	d.Add(event{f: f})
}

type event struct {
	f func()
}

func (e event) ToWork() error {
	e.f()
	return nil
}

// Wait 等待结果完成 timeout 超时时间
func (d *Dispatcher) Wait(timeout ...time.Duration) bool {
	if d.JobTotal == 0 {
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
	if !d.run {
		d.rm.Lock()
		if !d.run {
			d.run = true
			//调度器开始工作
			go d.dispatch()
		}
		d.rm.Unlock()
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

//调度
func (d *Dispatcher) dispatch() {
	for {
		//工作饱和
		if d.executions >= d.MaxWorkers {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		//获取工作
		job := d.JobQueue.Pop()
		//没有工作内容，停止工作调度
		if job == nil {
			d.run = false
			return
		}
		d.tally.Lock()
		d.executions++
		d.tally.Unlock()
		go func(w Job) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println(r)
				}
			}()
			if !d.stop {
				w.ToWork()
			}
			d.tally.Lock()
			d.executions--
			if d.executions < 0 {
				d.executions = 0
			}
			d.JobTotal--
			if d.JobTotal <= 0 {
				d.JobTotal = 0
				if d.stop {
					d.quit <- true
				} else if d.complete != nil {
					d.complete <- false
				}
			}
			d.tally.Unlock()
		}(job)
	}
}
