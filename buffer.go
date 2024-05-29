package buffer

import (
	"github.com/dongrv/iterator"
	"google.golang.org/protobuf/proto"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Reader interface {
	Read(x int64) proto.Message
}

type Writer interface {
	Write(proto.Message) int64
}

type ReaderWriter interface {
	Reader
	Writer
}

const (
	stdTidyLen = 10 // 设定标准长度
	stdTidyCap = 20 // 设定标准容量
	stdTimeout = 10 // 设定失效时间
	stdVisits  = 5  // 最大访问次数
)

type Buffer struct {
	store sync.Map
	iter  iterator.Iterator
	mu    sync.RWMutex
	tidy  map[int64]*metric
}

func New() *Buffer {
	return &Buffer{
		iter: iterator.New(),
		tidy: make(map[int64]*metric, stdTidyLen),
	}
}

func (b *Buffer) Read(x int64) proto.Message {
	value, ok := b.store.Load(x)
	if !ok {
		return nil
	}
	if !b.tidy[x].Can(nowUnix()) {
		return nil
	}
	b.tidy[x].Incr()
	return value.(proto.Message)
}

func (b *Buffer) Write(msg proto.Message) int64 {
	x := b.iter.Value()
	b.store.Store(x, msg)
	b.mu.Lock()
	b.tidy[x] = newMetric(x).Incr()
	b.mu.Unlock()
	b.Tidy()
	return x
}

// Tidy 整理删除无效元素
func (b *Buffer) Tidy() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.tidy) <= stdTidyCap {
		return
	}

	available := make([]*metric, 0, len(b.tidy))
	t := nowUnix()

	for x, m := range b.tidy {
		if !m.Can(t) {
			b.store.Delete(x)
			delete(b.tidy, x)
			continue
		}
		available = append(available, m)
	}

	sort.Slice(available, func(i, j int) bool {
		return available[i].recent < available[j].recent
	})

	if len(b.tidy) > stdTidyLen {
		for i := 0; i < len(b.tidy)-stdTidyLen; i++ {
			b.store.Delete(available[i].x)
			delete(b.tidy, available[i].x)
		}
	}
}

type metric struct {
	x      int64         // 所属序号
	recent time.Duration // 最近使用时间
	used   int64         // 使用次数
}

func newMetric(x int64) *metric {
	return &metric{x: x}
}

// Incr 计数和刷新
func (m *metric) Incr() *metric {
	atomic.AddInt64(&m.used, 1)
	m.recent = nowUnix()
	return m
}

// Can 是否可用
func (m *metric) Can(t time.Duration) bool {
	a := t.Seconds()-m.recent.Seconds() <= stdTimeout
	b := atomic.LoadInt64(&m.used) <= stdVisits
	//return t.Seconds()-m.recent.Seconds() <= stdTimeout || atomic.LoadInt64(&m.used) <= stdVisits
	return a && b
}

// 当前秒级时间戳
func nowUnix() time.Duration {
	return time.Duration(time.Now().Unix()) * time.Second
}
