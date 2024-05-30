package buffer

import (
	"errors"
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
	stdLen     = 10 // 设定标准长度
	stdCap     = 20 // 设定标准容量
	stdTimeout = 10 // 设定失效时间
	stdVisits  = 5  // 最大访问次数
)

type Buffer struct {
	op    *Option
	store sync.Map
	iter  iterator.Iterator
	mu    sync.RWMutex
	tidy  map[int64]*metric
}

func New(options ...OptionFunc) (*Buffer, error) {
	b := &Buffer{op: &Option{}, iter: iterator.New()}
	for _, fn := range options {
		fn(b.op)
	}
	if !b.op.Validate() {
		return nil, errors.New("invalid option value")
	}
	b.tidy = make(map[int64]*metric, b.op.Len)
	return b, nil
}

func (b *Buffer) Read(x int64) proto.Message {
	value, ok := b.store.Load(x)
	if !ok {
		return nil
	}
	if !b.tidy[x].can(b.op, nowUnix()) {
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

	if len(b.tidy) <= stdCap {
		return
	}

	available := make([]*metric, 0, len(b.tidy))
	t := nowUnix()

	for x, m := range b.tidy {
		if !m.can(b.op, t) {
			b.store.Delete(x)
			delete(b.tidy, x)
			continue
		}
		available = append(available, m)
	}

	sort.Slice(available, func(i, j int) bool {
		return available[i].recent < available[j].recent
	})

	if len(b.tidy) > stdLen {
		for i := 0; i < len(b.tidy)-stdLen; i++ {
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
func (m *metric) can(op *Option, t time.Duration) bool {
	return t.Seconds()-m.recent.Seconds() <= op.Timeout && atomic.LoadInt64(&m.used) <= op.Limit
}

type Option struct {
	Len     int     // 理想/健康长度
	Cap     int     // 容量，超过容量会触发缩容到Len
	Timeout float64 // 缓存过期时间
	Limit   int64   // 缓存最大访问次数限制
}

func DefaultOption() OptionFunc {
	return func(op *Option) {
		op.Len = stdLen
		op.Cap = stdCap
		op.Timeout = stdTimeout
		op.Limit = stdVisits
	}
}

func (op *Option) Validate() bool {
	return (op.Len > 0 && op.Timeout > 0 && op.Limit > 0) && (op.Cap > op.Len)
}

type OptionFunc func(option *Option)

// 当前秒级时间戳
func nowUnix() time.Duration {
	return time.Duration(time.Now().Unix()) * time.Second
}
