package queue

import "sync"

type Message[T any] struct {
	mes   sync.Map
	queue *Queue[*message[T]]
}

type message[T any] struct {
	state int32 //1正常 0回收 -1删除
	pop   bool
	value T
}

// MessagePool 声明消息池；利用sync.Map以及queue实现数据的重复利用
// 1:Get
// 2:Store
// 3:Load
// 4:DeletePush
func MessagePool[T any]() *Message[T] {
	return &Message[T]{queue: New[*message[T]]()}
}

//Load 返回存储在映射中的键值，如果否，则返回nil
func (m *Message[T]) Load(key any) (T, bool) {
	var t T
	if v, ok := m.mes.Load(key); ok {
		mm := v.(*message[T])
		if mm.state == 1 {
			return mm.value, true
		}
	}
	return t, false
}

//Get 获取一个已存在的数据
func (m *Message[T]) Get() *message[T] {
	for {
		mm := m.queue.Pop()
		if mm == nil {
			return nil
		}
		if mm.state == -1 {
			continue
		}
		return mm
	}
}

//Value 获取值
func (mm *message[T]) Value() T {
	var null T
	if mm == nil {
		return null
	}
	mm.state = 1
	mm.pop = true
	return mm.value
}

//Check 检查数据是否是可用状态
func (mm *message[T]) Check() bool {
	if mm == nil {
		return false
	}
	return mm.state == 1
}

//Put 放入
func (m *Message[T]) Put(mm *message[T]) {
	mm.pop = false
	m.queue.Push(mm)
}

// Store 设置键的值
func (m *Message[T]) Store(key any, value T) {
	if v, ok := m.mes.Load(key); ok {
		mm := v.(*message[T])
		mm.state = 1
		mm.value = value
		return
	}
	m.mes.Store(key, &message[T]{state: 1, value: value})
}

// Range 范围为映射中存在的每个键和值顺序调用。
// 如果返回false，则range停止迭代。
func (m *Message[T]) Range(f func(key any, value T) bool) {
	m.mes.Range(func(key, value any) bool {
		mm := value.(*message[T])
		if mm.state != 1 {
			return true
		}
		return f(key, mm.value)
	})
}

// Count 返回数量
func (m *Message[T]) Count() int {
	var count int
	m.mes.Range(func(key, value any) bool {
		mm := value.(*message[T])
		if mm.state != 1 {
			return true
		}
		count++
		return true
	})
	return count
}

// Delete 删除key；并未真正意义删除，以供重复使用来提高利用率
func (m *Message[T]) Delete(key any) bool {
	if v, ok := m.mes.Load(key); ok {
		mm := v.(*message[T])
		mm.state = 0
		if mm.pop {
			m.Put(mm)
		}
		return true
	}
	return false
}

// DeletePush 删除并存放key；并未真正意义删除，以供重复使用来提高利用率
func (m *Message[T]) DeletePush(key any) bool {
	if v, ok := m.mes.Load(key); ok {
		mm := v.(*message[T])
		mm.state = 0
		m.queue.Push(mm)
		return true
	}
	return false
}

// Clear 清除key
func (m *Message[T]) Clear(key any) {
	if v, ok := m.mes.Load(key); ok {
		mm := v.(*message[T])
		mm.state = -1
		m.mes.Delete(key)
	}
}

// DeleteCount 被删除的返回数量
func (m *Message[T]) DeleteCount() int {
	var count int
	m.mes.Range(func(key, value any) bool {
		mm := value.(*message[T])
		if mm.state == 1 {
			return true
		}
		count++
		return true
	})
	return count
}
