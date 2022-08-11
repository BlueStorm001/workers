package queue

import "sync"

type cell[T any] struct {
	data     []T      // 数据部分
	fullFlag bool     // cell满的标志
	next     *cell[T] // 指向后一个cell
	pre      *cell[T] // 指向前一个cell
	r        int      // 下一个要读的指针
	w        int      // 下一个要下的指针
	l        int
}

// Queue RingQueue
type Queue[T any] struct {
	cellCount int      // cell 数量统计
	readCell  *cell[T] // 下一个要读的cell
	writeCell *cell[T] // 下一个要写的cell
	wm        sync.Mutex
	rm        sync.Mutex
	grows     bool
}

var CellSize = 100
var CellCount = 2

func Create[T any](size int, grows ...bool) *Queue[T] {
	q := New[T](size)
	if len(grows) > 0 {
		q.grows = grows[0]
	}
	return q
}

func New[T any](size ...int) *Queue[T] {
	if len(size) > 0 {
		CellSize = size[0]
	}
	rootCell := &cell[T]{
		data: make([]T, CellSize),
	}
	lastCell := &cell[T]{
		data: make([]T, CellSize),
	}
	rootCell.pre = lastCell
	lastCell.pre = rootCell
	rootCell.next = lastCell
	lastCell.next = rootCell
	return &Queue[T]{
		cellCount: CellCount,
		readCell:  rootCell,
		writeCell: rootCell,
	}
}

// FILO 先进后出
func (q *Queue[T]) FILO() T {
	var null T
	// 无数据
	if q.IsEmpty() {
		return null
	}
	q.rm.Lock()
	if q.readCell.l == 0 {
		q.readCell.l = q.readCell.w
	}
	q.readCell.l--
	if q.readCell.l < 0 {
		return null
	}
	// 读取数据，并将读指针向右移动一位
	value := q.readCell.data[q.readCell.l]
	q.readCell.r++
	if q.readCell.r == q.readCell.w {
		q.readCell.l = 0
	}
	// 此cell已经读完
	if q.readCell.r == CellSize {
		// 读指针归零，并将该cell状态置为非满
		q.readCell.r = 0
		q.readCell.fullFlag = false
		// 将readCell指向下一个cell
		q.readCell = q.readCell.next
	}
	q.rm.Unlock()
	return value
}

// Pop 先进先出 FIFO
func (q *Queue[T]) Pop() T {
	// 无数据
	if q.IsEmpty() {
		var null T
		return null
	}
	q.rm.Lock()
	// 读取数据，并将读指针向右移动一位
	value := q.readCell.data[q.readCell.r]
	q.readCell.r++
	// 此cell已经读完
	if q.readCell.r == CellSize {
		// 读指针归零，并将该cell状态置为非满
		q.readCell.r = 0
		q.readCell.fullFlag = false
		// 将readCell指向下一个cell
		q.readCell = q.readCell.next
	}
	q.rm.Unlock()
	return value
}

// Peek 窥视 读一个元素，仅读但不移动指针
func (q *Queue[T]) Peek(index int) T {
	total := len(q.readCell.data)
	if index >= total {
		index = total - 1
	}
	if index < 0 {
		index = q.readCell.r
	}
	return q.readCell.data[index]
}

// Push 写入数据
func (q *Queue[T]) Push(value T) bool {
	q.wm.Lock()
	// 在 r.writeCell.w 位置写入数据，指针向右移动一位
	q.writeCell.data[q.writeCell.w] = value
	q.writeCell.w++
	// 当前cell写满了
	if q.writeCell.w == CellSize {
		// 指针置0，将该cell标记为已满，并指向下一个cell
		q.writeCell.w = 0
		q.writeCell.fullFlag = true
		q.writeCell = q.writeCell.next
	}
	// 下一个cell也已满，扩容
	if q.writeCell.fullFlag == true {
		q.grow()
	}
	q.wm.Unlock()
	return true
}

// grow 扩容
func (q *Queue[T]) grow() {
	// 新建一个cell
	newCell := &cell[T]{
		data: make([]T, CellSize),
	}
	// 总共三个cell，writeCell，preCell，newCell
	// 本来关系： preCell <===> writeCell
	// 现在将newcell插入：preCell <===> newCell <===> writeCell
	pre := q.writeCell.pre
	pre.next = newCell
	newCell.pre = pre
	newCell.next = q.writeCell
	q.writeCell.pre = newCell
	// 将writeCell指向新建的cell
	q.writeCell = q.writeCell.pre
	// cell 数量加一
	q.cellCount++
}

func (q *Queue[T]) IsEmpty() bool {
	// readCell和writeCell指向同一个cell，并且该cell的读写指针也指向同一个位置，并且cell状态为非满
	if q.readCell == q.writeCell && q.readCell.r == q.readCell.w && q.readCell.fullFlag == false {
		return true
	}
	return false
}

// Capacity 容量
func (q *Queue[T]) Capacity() int {
	return q.cellCount * CellSize
}

// Reset 重置为仅指向两个cell的ring
func (q *Queue[T]) Reset() {
	lastCell := q.readCell.next
	lastCell.w = 0
	lastCell.r = 0
	q.readCell.r = 0
	q.readCell.w = 0
	q.cellCount = CellCount
	lastCell.next = q.readCell
}
