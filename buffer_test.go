package buffer

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"strconv"
	"testing"
	"time"
)

type TestMessage struct {
	proto.Message
	A string
	B int64
}

func mockBufferMessage(target int) []TestMessage {
	result := make([]TestMessage, 0, target)
	for i := 0; i < target; i++ {
		result = append(result, TestMessage{A: strconv.Itoa(i), B: int64(i * 10)})
	}
	return result
}

func TestBuffer_Read(t *testing.T) {
	b, _ := New(DefaultOption())
	// 测试过期
	x := b.Write(TestMessage{A: "1", B: 2})
	_, ok := b.Read(x).(TestMessage)
	assert.True(t, ok)
	debugWaitSecond(stdTimeout + 1)
	_, ok = b.Read(x).(TestMessage)
	assert.False(t, ok)
	// 测试次数
	x2 := b.Write(TestMessage{A: "1", B: 2})
	for i := 0; i < stdVisits-1; i++ {
		b.Read(x2)
	}
	_, ok = b.Read(x2).(TestMessage)
	assert.True(t, ok)
	_, ok = b.Read(x2).(TestMessage)
	assert.False(t, ok)
}

func TestBuffer_Write(t *testing.T) {
	b, _ := New(DefaultOption())
	msgs := mockBufferMessage(stdLen)
	seq := make([]int64, 0, stdLen)
	for _, message := range msgs {
		seq = append(seq, b.Write(message))
	}
	for _, v := range seq {
		buf := b.Read(v)
		_, ok := buf.(TestMessage)
		assert.True(t, ok)
	}
}

func TestBuffer_Tidy(t *testing.T) {
	b, _ := New(DefaultOption())
	msgs := mockBufferMessage(2 * stdLen)
	seq := make([]int64, 0, 2*stdLen)
	for _, message := range msgs {
		seq = append(seq, b.Write(message))
		debugWaitSecond(1)
	}
	for _, v := range seq {
		buf := b.Read(v)
		_, ok := buf.(TestMessage)
		if v <= stdLen { // 前10个过期
			assert.False(t, ok)
			continue
		}
		assert.True(t, ok)
	}
}

func TestMetric_can(t *testing.T) {
	buffer, _ := New(DefaultOption())
	// 测试过期
	m := newMetric(1).Incr()
	assert.True(t, m.can(buffer.op, nowUnix()))
	debugWaitSecond(stdTimeout + 1)
	assert.False(t, m.can(buffer.op, nowUnix()))
	// 测试次数
	m2 := newMetric(2)
	for i := 0; i < stdVisits; i++ {
		m2.Incr()
	}
	assert.True(t, m2.can(buffer.op, nowUnix()))
	m2.Incr()
	assert.False(t, m2.can(buffer.op, nowUnix()))
}

// debugWaitSecond 等待时间
func debugWaitSecond(t int64) {
	time.Sleep(time.Duration(t) * time.Second)
}

func TestSingle_Read(t *testing.T) {
	single, err := NewSingle()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := &TestMessage{A: "1", B: 2}
	x := single.Write(msg)
	if x <= 0 {
		t.Fatalf("unexpected %v", x)
	}
	// 测试过期
	assert.Equal(t, msg, single.Read(x))
	debugWaitSecond(stdTimeout + 1)
	assert.Equal(t, nil, single.Read(x))
	// 测试次数
	x = single.Write(msg)
	for i := 0; i < stdVisits-1; i++ {
		single.Read(x)
	}
	assert.Equal(t, msg, single.Read(x))
	assert.Equal(t, nil, single.Read(x))
	// 测试序号
	assert.Equal(t, int64(3), single.Write(msg))
}
