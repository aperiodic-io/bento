package pure_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"

	"github.com/warpstreamlabs/bento/public/service"

	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

func parseYAMLConf(t testing.TB, formatStr string, args ...any) (conf processor.Config) {
	t.Helper()
	var err error
	conf, err = testutil.ProcessorFromYAML(fmt.Sprintf(formatStr, args...))
	require.NoError(t, err)
	return
}

// testFuncs are the functions pure_test_func processors run, by name.
var testFuncs sync.Map

// pure_test_func runs the function of its name on each message: its result
// replaces the message, and its error flags it.
func init() {
	err := service.RegisterProcessor("pure_test_func", service.NewConfigSpec().Field(service.NewStringField("name")),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
			name, err := conf.FieldString("name")
			if err != nil {
				return nil, err
			}
			fn, ok := testFuncs.Load(name)
			if !ok {
				return nil, fmt.Errorf("no test function %v", name)
			}
			return testFuncProc(fn.(func(context.Context, []byte) ([]byte, error))), nil
		})
	if err != nil {
		panic(err)
	}
}

type testFuncProc func(context.Context, []byte) ([]byte, error)

func (f testFuncProc) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	b, err := m.AsBytes()
	if err != nil {
		return nil, err
	}
	out, err := f(ctx, b)
	if err != nil {
		return nil, err
	}
	m.SetBytes(out)
	return service.MessageBatch{m}, nil
}

func (f testFuncProc) Close(context.Context) error { return nil }

// testFunc registers fn for a pure_test_func processor and returns its name.
func testFunc(t *testing.T, fn func(context.Context, []byte) ([]byte, error)) string {
	testFuncs.Store(t.Name(), fn)
	t.Cleanup(func() { testFuncs.Delete(t.Name()) })
	return t.Name()
}

func TestParallelBasic(t *testing.T) {
	// every message waits for all five: they are processed at once
	wg := sync.WaitGroup{}
	wg.Add(5)
	name := testFunc(t, func(context.Context, []byte) ([]byte, error) {
		wg.Done()
		wg.Wait()
		return []byte("foobar"), nil
	})

	conf := parseYAMLConf(t, `
parallel:
  processors:
    - pure_test_func:
        name: %v
`, name)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestParallelError(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(5)
	name := testFunc(t, func(_ context.Context, b []byte) ([]byte, error) {
		wg.Done()
		wg.Wait()
		if string(b) == "baz" {
			return nil, errors.New("test error")
		}
		return []byte("foobar"), nil
	})

	conf := parseYAMLConf(t, `
parallel:
  processors:
    - pure_test_func:
        name: %v
`, name)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
	}))
	if res != nil {
		t.Error(res)
	}
	if expC, actC := 5, msgs[0].Len(); actC != expC {
		t.Fatalf("Wrong result count: %v != %v", actC, expC)
	}
	if exp, act := "baz", string(msgs[0].Get(2).AsBytes()); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	assert.Error(t, msgs[0].Get(2).ErrorGet())
	for _, i := range []int{0, 1, 3, 4} {
		if exp, act := "foobar", string(msgs[0].Get(i).AsBytes()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
		assert.NoError(t, msgs[0].Get(i).ErrorGet())
	}
}

func TestParallelCapped(t *testing.T) {
	var reqs atomic.Int64
	name := testFunc(t, func(context.Context, []byte) ([]byte, error) {
		if req := reqs.Add(1); req > 5 {
			t.Errorf("Beyond parallelism cap: %v", req)
		}
		<-time.After(time.Millisecond * 10)
		reqs.Add(-1)
		return []byte("foobar"), nil
	})

	conf := parseYAMLConf(t, `
parallel:
  cap: 5
  processors:
    - pure_test_func:
        name: %v
`, name)

	h, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := h.ProcessBatch(context.Background(), message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("qux"),
		[]byte("quz"),
		[]byte("foo2"),
		[]byte("bar2"),
		[]byte("baz2"),
		[]byte("qux2"),
		[]byte("quz2"),
	}))
	if res != nil {
		t.Error(res)
	} else if expC, actC := 10, msgs[0].Len(); actC != expC {
		t.Errorf("Wrong result count: %v != %v", actC, expC)
	} else if exp, act := "foobar", string(message.GetAllBytes(msgs[0])[0]); act != exp {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}
