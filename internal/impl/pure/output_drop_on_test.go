package pure_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/internal/component/output"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	bmock "github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func parseYAMLOutputConf(t testing.TB, formatStr string, args ...any) output.Config {
	t.Helper()
	conf, err := testutil.OutputFromYAMLWithArgs(formatStr, args...)
	require.NoError(t, err)
	return conf
}

func TestDropOnNothing(t *testing.T) {
	dropConf := parseYAMLOutputConf(t, `
drop_on:
  error: false
  output:
    reject: test error
`)

	d, err := bmock.NewManager().NewOutput(dropConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		d.TriggerCloseNow()
		assert.NoError(t, d.WaitForClose(ctx))
		done()
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan error)

	require.NoError(t, d.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foobar")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var res error
	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	assert.EqualError(t, res, "test error")
}

func TestDropOnError(t *testing.T) {
	dropConf := parseYAMLOutputConf(t, `
drop_on:
  error: true
  output:
    reject: test error
`)

	d, err := bmock.NewManager().NewOutput(dropConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		d.TriggerCloseNow()
		assert.NoError(t, d.WaitForClose(ctx))
		done()
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan error)

	require.NoError(t, d.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foobar")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var res error
	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	assert.NoError(t, res)
}

func TestDropOnErrorMatches(t *testing.T) {
	// the message is the error, as the body of a response the output fails on
	dropConf := parseYAMLOutputConf(t, `
drop_on:
  error_patterns:
    - foobar
  output:
    reject: '${! content() }'
`)

	d, err := bmock.NewManager().NewOutput(dropConf)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		d.TriggerCloseNow()
		assert.NoError(t, d.WaitForClose(ctx))
		done()
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan error)

	require.NoError(t, d.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("error doesnt match")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var res error
	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	require.Error(t, res)
	assert.Contains(t, res.Error(), "error doesnt match")

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foobar")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	assert.NoError(t, res)
}
