package pure_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/manager/mock"
	"github.com/warpstreamlabs/bento/internal/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestRejectErroredHappy(t *testing.T) {
	dir := t.TempDir()

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = content().uppercase()'
fallback:
  - reject_errored:
      file:
        path: $URL/a
        codec: lines
  - file:
      path: $URL/dlq
      codec: lines
`, "$URL", dir))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test a",
		},
		{
			"test b",
			"test c",
			"test d",
			"test e",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	results := fileResults(t, dir)
	assert.Equal(t, map[string][]string{
		"/a": {
			"TEST A",
			"TEST B",
			"TEST C",
			"TEST D",
			"TEST E",
		},
	}, results)
}

func TestRejectErroredSad(t *testing.T) {
	dir := t.TempDir()

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      file:
        path: $URL/a
        codec: lines
  - file:
      path: $URL/dlq
      codec: lines
`, "$URL", dir))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
		},
		{
			"test b",
			"test nope c",
			"test d",
			"test nope e",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	results := fileResults(t, dir)
	assert.Equal(t, map[string][]string{
		"/a": {
			"test b",
			"test d",
		},
		"/dlq": {
			"test nope a",
			"test nope c",
			"test nope e",
		},
	}, results)
}

func TestRejectErroredSadWholeBatch(t *testing.T) {
	dir := t.TempDir()

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      file:
        path: $URL/a
        codec: lines
  - file:
      path: $URL/dlq
      codec: lines
`, "$URL", dir))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
			"test nope b",
			"test nope c",
			"test nope d",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	results := fileResults(t, dir)
	assert.Equal(t, map[string][]string{
		"/dlq": {
			"test nope a",
			"test nope b",
			"test nope c",
			"test nope d",
		},
	}, results)
}

func TestRejectErroredNestedBatchErrors(t *testing.T) {
	dir := t.TempDir()

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      reject_errored:
        file:
          path: $URL/a
          codec: lines
      processors:
        - mapping: 'root = if content().contains("nah") { throw("nuh uh") }'
  - file:
      path: $URL/dlq
      codec: lines
`, "$URL", dir))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
			"test b",
			"test nah c",
			"test nope d",
		},
		{
			"test nah e",
		},
		{
			"test nah f",
			"test nah g",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	results := fileResults(t, dir)
	assert.Equal(t, map[string][]string{
		"/a": {
			"test b",
		},
		"/dlq": {
			"test nope a",
			"test nah c",
			"test nope d",
			"test nah e",
			"test nah f",
			"test nah g",
		},
	}, results)
}

func TestRejectErroredNestedTotalErrors(t *testing.T) {
	dir := t.TempDir()

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      reject: "everything"
  - file:
      path: $URL/dlq
      codec: lines
`, "$URL", dir))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
			"test b",
			"test c",
			"test nope d",
		},
		{
			"test nope e",
		},
		{
			"test f",
		},
		{
			"test g",
			"test h",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	results := fileResults(t, dir)
	assert.Equal(t, map[string][]string{
		"/dlq": {
			"test nope a",
			"test b",
			"test c",
			"test nope d",
			"test nope e",
			"test f",
			"test g",
			"test h",
		},
	}, results)
}

// fileResults is what the file outputs these tests write to wrote under dir,
// one message a line, by the path each wrote to.
func fileResults(t *testing.T, dir string) map[string][]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	results := map[string][]string{}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		results["/"+e.Name()] = strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	}
	return results
}
