package queue

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"unsafe"
)

type myTester struct {
	key int
	s   string
	i   int
}

var mes = MessagePool[*myTester]()

var id int

func TestWorkflow(t *testing.T) {
	for i := 0; i < 10; i++ {
		//先拿 为了重复利用
		myStruct := mes.Get().Value()
		if myStruct == nil {
			myStruct = &myTester{}
		}
		//注意：key 尽量重复 利用默认值给予判断
		if myStruct.key == 0 {
			id++
			myStruct.key = id
		}
		myStruct.s = "+" + strconv.Itoa(i)
		//再存
		mes.Store(myStruct.key, myStruct)
		//使用
		v, ok := mes.Load(myStruct.key)
		fmt.Println("使用", ok, v.s, unsafe.Pointer(v))
		//删掉并放入
		mes.DeletePush(myStruct.key)
		//验证
		v, ok = mes.Load(myStruct.key)
		fmt.Println("删除", ok, v)
	}
}

func TestMessagePool(t *testing.T) {
	m := &myTester{key: 1}
	mes.Store(m.key, m)
	if v, ok := mes.Load(m.key); ok {
		fmt.Println(v.key)
	}
	mes.Delete(m.key)
	myt := mes.Get()
	fmt.Println(myt)
	if v, ok := mes.Load(m.key); ok {
		fmt.Println(v.key)
	}
	mes.Store(m.key, m)
	if v, ok := mes.Load(m.key); ok {
		fmt.Println(v.key)
	}

	mes.Range(func(key any, value *myTester) bool {
		fmt.Println(value.key)
		return true
	})
	count := mes.Count()
	fmt.Println(count)
}

func BenchmarkMessagePool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		myStruct := mes.Get().Value()
		if myStruct == nil {
			myStruct = &myTester{}
		}
		if myStruct.key == 0 {
			myStruct.key = i
		}
		myStruct.i = i
		mes.Store(myStruct.key, myStruct)
		mes.DeletePush(myStruct.key)
	}
}

var smap sync.Map

func BenchmarkMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := &myTester{key: 1}
		smap.Store(m.key, m)
		smap.Delete(m.key)
	}
}
