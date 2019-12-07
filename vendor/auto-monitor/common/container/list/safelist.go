package list

import (
	"container/list"
	"sync"
)

type SafeList struct {
	sync.Mutex
	l *list.List
}

func NewSafeList() *SafeList {
	return &SafeList{l: list.New()}
}

func (sl *SafeList) PushFront(v interface{}) *list.Element {
	sl.Lock()
	e := sl.l.PushFront(v)
	sl.Unlock()
	return e
}

func (sl *SafeList) PushFrontBatch(vs []interface{}) {
	sl.Lock()
	for _, item := range vs {
		sl.l.PushFront(item)
	}
	sl.Unlock()
}

func (sl *SafeList) PopBack() interface{} {
	sl.Lock()
	if elem := sl.l.Back(); elem != nil {
		item := sl.l.Remove(elem)
		sl.Unlock()
		return item
	}
	sl.Unlock()
	return nil
}

func (sl *SafeList) PopBackBy(max int) []interface{} {
	sl.Lock()

	count := sl.l.Len()
	if count == 0 {
		sl.Unlock()
		return []interface{}{}
	}

	if count > max {
		count = max
	}

	items := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		item := sl.l.Remove(sl.l.Back())
		items = append(items, item)
	}

	sl.Unlock()
	return items
}

func (sl *SafeList) PopBackAll() []interface{} {
	sl.Lock()

	count := sl.l.Len()
	if count == 0 {
		sl.Unlock()
		return []interface{}{}
	}

	items := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		item := sl.l.Remove(sl.l.Back())
		items = append(items, item)
	}

	sl.Unlock()
	return items
}

func (sl *SafeList) Remove(e *list.Element) interface{} {
	sl.Lock()
	defer sl.Unlock()
	return sl.l.Remove(e)
}

func (sl *SafeList) RemoveAll() {
	sl.Lock()
	sl.l = list.New()
	sl.Unlock()
}

func (sl *SafeList) Front() interface{} {
	sl.Lock()
	if f := sl.l.Front(); f != nil {
		sl.Unlock()
		return f.Value
	}
	sl.Unlock()
	return nil
}

func (sl *SafeList) FrontAll() []interface{} {
	sl.Lock()
	defer sl.Unlock()

	count := sl.l.Len()
	if count == 0 {
		return []interface{}{}
	}

	items := make([]interface{}, 0, count)
	for e := sl.l.Front(); e != nil; e = e.Next() {
		items = append(items, e.Value)
	}
	return items
}

func (sl *SafeList) BackAll() []interface{} {
	sl.Lock()
	defer sl.Unlock()

	count := sl.l.Len()
	if count == 0 {
		return []interface{}{}
	}

	items := make([]interface{}, 0, count)
	for e := sl.l.Back(); e != nil; e = e.Prev() {
		items = append(items, e.Value)
	}
	return items
}

func (sl *SafeList) Len() int {
	sl.Lock()
	defer sl.Unlock()
	return sl.l.Len()
}

// SafeList with Limited Size
type SafeListLimited struct {
	sync.Mutex
	maxSize int
	l       *list.List
}

func NewSafeListLimited(maxSize int) *SafeListLimited {
	return &SafeListLimited{l: list.New(), maxSize: maxSize}
}

func (sl *SafeListLimited) PushFront(v interface{}) bool {
	sl.Lock()
	if sl.l.Len() >= sl.maxSize {
		sl.Unlock()
		return false
	}

	sl.l.PushFront(v)
	sl.Unlock()
	return true
}

func (sl *SafeListLimited) PushFrontBatch(vs []interface{}) bool {
	sl.Lock()
	if sl.l.Len() >= sl.maxSize {
		sl.Unlock()
		return false
	}

	for _, item := range vs {
		sl.l.PushFront(item)
	}
	sl.Unlock()
	return true
}

func (sl *SafeListLimited) PopBack() interface{} {
	sl.Lock()
	if elem := sl.l.Back(); elem != nil {
		item := sl.l.Remove(elem)
		sl.Unlock()
		return item
	}
	sl.Unlock()
	return nil
}

func (sl *SafeListLimited) PopBackBy(max int) []interface{} {
	sl.Lock()

	count := sl.l.Len()
	if count == 0 {
		sl.Unlock()
		return []interface{}{}
	}

	if count > max {
		count = max
	}

	items := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		item := sl.l.Remove(sl.l.Back())
		items = append(items, item)
	}

	sl.Unlock()
	return items
}

func (sl *SafeListLimited) Remove(e *list.Element) interface{} {
	sl.Lock()
	defer sl.Unlock()
	return sl.l.Remove(e)
}

func (sl *SafeListLimited) RemoveAll() {
	sl.Lock()
	sl.l = list.New()
	sl.Unlock()
}

func (sl *SafeListLimited) Front() interface{} {
	sl.Lock()
	if f := sl.l.Front(); f != nil {
		sl.Unlock()
		return f.Value
	}
	sl.Unlock()
	return nil
}

func (sl *SafeListLimited) FrontAll() []interface{} {
	sl.Lock()
	defer sl.Unlock()

	count := sl.l.Len()
	if count == 0 {
		return []interface{}{}
	}

	items := make([]interface{}, 0, count)
	for e := sl.l.Front(); e != nil; e = e.Next() {
		items = append(items, e.Value)
	}
	return items
}

func (sl *SafeListLimited) Len() int {
	sl.Lock()
	defer sl.Unlock()
	return sl.l.Len()
}
