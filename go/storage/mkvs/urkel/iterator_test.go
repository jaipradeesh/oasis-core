package urkel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/node"
	"github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/syncer"
	"github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/writelog"
)

func TestIterator(t *testing.T) {
	ctx := context.Background()
	tree := New(nil, nil)
	defer tree.Close()

	// Test with an empty tree.
	it := tree.NewIterator(ctx)
	defer it.Close()

	it.Rewind()
	require.False(t, it.Valid(), "iterator should be invalid on an empty tree")

	// Test with one item.
	err := tree.Insert(ctx, []byte("key"), []byte("first"))
	require.NoError(t, err, "Insert")

	it.Rewind()
	require.True(t, it.Valid(), "iterator should valid on a non-empty tree")

	// Insert some items.
	items := writelog.WriteLog{
		writelog.LogEntry{Key: []byte("key"), Value: []byte("first")},
		writelog.LogEntry{Key: []byte("key 1"), Value: []byte("one")},
		writelog.LogEntry{Key: []byte("key 2"), Value: []byte("two")},
		writelog.LogEntry{Key: []byte("key 5"), Value: []byte("five")},
		writelog.LogEntry{Key: []byte("key 8"), Value: []byte("eight")},
		writelog.LogEntry{Key: []byte("key 9"), Value: []byte("nine")},
	}

	tests := []testCase{
		{seek: node.Key("k"), pos: 0},
		{seek: node.Key("key 1"), pos: 1},
		{seek: node.Key("key 3"), pos: 3},
		{seek: node.Key("key 4"), pos: 3},
		{seek: node.Key("key 5"), pos: 3},
		{seek: node.Key("key 6"), pos: 4},
		{seek: node.Key("key 7"), pos: 4},
		{seek: node.Key("key 8"), pos: 4},
		{seek: node.Key("key 9"), pos: 5},
		{seek: node.Key("key A"), pos: -1},
	}

	err = tree.ApplyWriteLog(ctx, writelog.NewStaticIterator(items))
	require.NoError(t, err, "ApplyWriteLog")

	t.Run("Direct", func(t *testing.T) {
		dit := tree.NewIterator(ctx)
		defer dit.Close()
		testIterator(t, items, dit, tests)
	})

	var root node.Root
	_, rootHash, err := tree.Commit(ctx, root.Namespace, root.Round)
	require.NoError(t, err, "Commit")
	root.Hash = rootHash

	stats := syncer.NewStatsCollector(tree)
	remote := NewWithRoot(stats, nil, root)
	defer remote.Close()

	t.Run("Remote", func(t *testing.T) {
		rit := remote.NewIterator(ctx)
		defer rit.Close()

		testIterator(t, items, rit, tests)

		require.EqualValues(t, 0, stats.SyncGetCount, "SyncGetCount")
		require.EqualValues(t, 0, stats.SyncGetPrefixesCount, "SyncGetPrefixesCount")
		require.EqualValues(t, 6, stats.SyncIterateCount, "SyncIterateCount")
	})

	stats = syncer.NewStatsCollector(tree)
	remote = NewWithRoot(stats, nil, root)
	defer remote.Close()

	t.Run("RemoteWithPrefetch10", func(t *testing.T) {
		rpit := remote.NewIterator(ctx, IteratorPrefetch(10))
		defer rpit.Close()

		testIterator(t, items, rpit, tests)

		require.EqualValues(t, 0, stats.SyncGetCount, "SyncGetCount")
		require.EqualValues(t, 0, stats.SyncGetPrefixesCount, "SyncGetPrefixesCount")
		require.EqualValues(t, 1, stats.SyncIterateCount, "SyncIterateCount")
	})

	stats = syncer.NewStatsCollector(tree)
	remote = NewWithRoot(stats, nil, root)
	defer remote.Close()

	t.Run("RemoteWithPrefetch3", func(t *testing.T) {
		rpit := remote.NewIterator(ctx, IteratorPrefetch(3))
		defer rpit.Close()

		testIterator(t, items, rpit, tests)

		require.EqualValues(t, 0, stats.SyncGetCount, "SyncGetCount")
		require.EqualValues(t, 0, stats.SyncGetPrefixesCount, "SyncGetPrefixesCount")
		require.EqualValues(t, 2, stats.SyncIterateCount, "SyncIterateCount")
	})

	statsIntermediate := syncer.NewStatsCollector(tree)
	intermediate := NewWithRoot(statsIntermediate, nil, root)
	defer intermediate.Close()

	stats = syncer.NewStatsCollector(intermediate)
	remote = NewWithRoot(stats, nil, root)
	defer remote.Close()

	t.Run("RemoteIntermediateWithPrefetch10", func(t *testing.T) {
		rpit := remote.NewIterator(ctx, IteratorPrefetch(10))
		defer rpit.Close()

		testIterator(t, items, rpit, tests)

		require.EqualValues(t, 0, stats.SyncGetCount, "SyncGetCount")
		require.EqualValues(t, 0, stats.SyncGetPrefixesCount, "SyncGetPrefixesCount")
		require.EqualValues(t, 1, stats.SyncIterateCount, "SyncIterateCount")

		require.EqualValues(t, 0, statsIntermediate.SyncGetCount, "SyncGetCount")
		require.EqualValues(t, 0, statsIntermediate.SyncGetPrefixesCount, "SyncGetPrefixesCount")
		require.EqualValues(t, 1, statsIntermediate.SyncIterateCount, "SyncIterateCount")
	})
}

func TestIteratorEviction(t *testing.T) {
	ctx := context.Background()
	tree := New(nil, nil, Capacity(0, 0))
	defer tree.Close()

	keys, values := generateKeyValuePairsEx("T", 100)
	var items writelog.WriteLog
	for i, k := range keys {
		err := tree.Insert(ctx, k, values[i])
		require.NoError(t, err, "Insert")
		items = append(items, writelog.LogEntry{Key: k, Value: values[i]})
	}

	var root node.Root
	_, rootHash, err := tree.Commit(ctx, root.Namespace, root.Round)
	require.NoError(t, err, "Commit")
	root.Hash = rootHash

	// Create a remote tree with limited cache capacity so that nodes will
	// be evicted while iterating.
	stats := syncer.NewStatsCollector(tree)
	remote := NewWithRoot(stats, nil, root, Capacity(50, 16*1024*1024))
	defer remote.Close()

	it := remote.NewIterator(ctx, IteratorPrefetch(1000))
	defer it.Close()

	testIterator(t, items, it, nil)

	require.EqualValues(t, 0, stats.SyncGetCount, "SyncGetCount")
	require.EqualValues(t, 0, stats.SyncGetPrefixesCount, "SyncGetPrefixesCount")
	// We require multiple fetches as we can only store a limited amount of
	// results per fetch due to the cache being too small.
	require.EqualValues(t, 2, stats.SyncIterateCount, "SyncIterateCount")
}

type testCase struct {
	seek node.Key
	pos  int
}

func testIterator(t *testing.T, items writelog.WriteLog, it Iterator, tests []testCase) {
	// Iterate through the whole tree.
	var idx int
	for it.Rewind(); it.Valid(); it.Next() {
		if tests != nil {
			require.EqualValues(t, items[idx].Key, it.Key(), "iterator should have the correct key")
			require.EqualValues(t, items[idx].Value, it.Value(), "iterator should have the correct value")
		}
		idx++
	}
	require.NoError(t, it.Err(), "iterator should not error")
	require.EqualValues(t, len(items), idx, "iterator should go over all items")

	// Perform any specific tests.
	for _, tc := range tests {
		it.Seek(tc.seek)
		if tc.pos == -1 {
			require.False(t, it.Valid(), "iterator should not be valid after Seek")
			continue
		}

		for _, item := range items[tc.pos:] {
			require.True(t, it.Valid(), "iterator should be valid after Seek/Next")
			require.EqualValues(t, item.Key, it.Key(), "iterator should have the correct key")
			require.EqualValues(t, item.Value, it.Value(), "iterator should have the correct value")
			it.Next()
		}

		require.False(t, it.Valid(), "iterator should not be valid after reaching the end")
	}
}
