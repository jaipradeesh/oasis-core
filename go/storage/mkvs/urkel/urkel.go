// Package urkel provides an Urkel tree implementation.
package urkel

import (
	"context"
	"errors"

	db "github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/db/api"
	"github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/node"
	"github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/syncer"
	"github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/writelog"
)

var (
	// ErrClosed is the error returned when methods are used after Close is called.
	ErrClosed = errors.New("urkel: tree is closed")
	// ErrKnownRootMismatch is the error returned by CommitKnown when the known
	// root mismatches.
	ErrKnownRootMismatch = errors.New("urkel: known root mismatch")

	_ syncer.ReadSyncer = (*Tree)(nil)
)

// Tree is an Urkel tree.
type Tree struct {
	cache *cache

	// NOTE: This can be a map as updates are commutative.
	pendingWriteLog map[string]*pendingEntry
	// pendingRemovedNodes are the nodes that have been removed from the
	// in-memory tree and should be marked for garbage collection if this
	// tree is committed to the node database.
	pendingRemovedNodes []node.Node
}

type pendingEntry struct {
	key     []byte
	value   []byte
	existed bool

	insertedLeaf *node.Pointer
}

// Option is a configuration option used when instantiating the tree.
type Option func(t *Tree)

// Capacity sets the capacity of the in-memory cache.
//
// If no capacity is specified, the cache will have a maximum capacity of
// 16MB for values and 5000 for nodes.
//
// If a capacity of 0 is specified, the cache will have an unlimited size
// (not recommended, as this will cause unbounded memory growth).
func Capacity(nodeCapacity uint64, valueCapacityBytes uint64) Option {
	return func(t *Tree) {
		t.cache.nodeCapacity = nodeCapacity
		t.cache.valueCapacity = valueCapacityBytes
	}
}

// PersistEverythingFromSyncer sets whether to persist all the nodes and
// values obtained from the remote syncer to local database.
//
// If not specified, the default is false.
func PersistEverythingFromSyncer(doit bool) Option {
	return func(t *Tree) {
		t.cache.persistEverythingFromSyncer = doit
	}
}

// New creates a new empty Urkel tree backed by the given node database.
func New(rs syncer.ReadSyncer, ndb db.NodeDB, options ...Option) *Tree {
	if rs == nil {
		rs = syncer.NopReadSyncer
	}
	if ndb == nil {
		ndb, _ = db.NewNopNodeDB()
	}

	t := &Tree{
		cache:           newCache(ndb, rs),
		pendingWriteLog: make(map[string]*pendingEntry),
	}

	for _, v := range options {
		v(t)
	}

	return t
}

// NewWithRoot creates a new Urkel tree with an existing root, backed by
// the given node database.
func NewWithRoot(rs syncer.ReadSyncer, ndb db.NodeDB, root node.Root, options ...Option) *Tree {
	t := New(rs, ndb, options...)
	t.cache.setPendingRoot(&node.Pointer{
		Clean: true,
		Hash:  root.Hash,
	})
	t.cache.setSyncRoot(root)
	return t
}

// NewIterator returns a new iterator over the tree.
func (t *Tree) NewIterator(ctx context.Context, options ...IteratorOption) Iterator {
	return NewIterator(ctx, t, options...)
}

// ApplyWriteLog applies the operations from a write log to the current tree.
//
// The caller is responsible for calling Commit.
func (t *Tree) ApplyWriteLog(ctx context.Context, wl writelog.Iterator) error {
	for {
		// Fetch next entry from write log iterator.
		more, err := wl.Next()
		if err != nil {
			return err
		}
		if !more {
			break
		}
		entry, err := wl.Value()
		if err != nil {
			return err
		}

		// Apply operation.
		if len(entry.Value) == 0 {
			err = t.Remove(ctx, entry.Key)
		} else {
			err = t.Insert(ctx, entry.Key, entry.Value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Close releases resources associated with this tree. After calling this
// method the tree MUST NOT be used anymore and all methods will return
// the ErrClosed error.
//
// Any pending write operations are discarded. If you need to persist them
// you need to call Commit before calling this method.
func (t *Tree) Close() {
	t.cache.Lock()
	defer t.cache.Unlock()

	t.cache.close()
	t.pendingWriteLog = nil
}

// Size calculates the size of the tree in bytes.
func (t *Tree) Size() uint64 {
	return t.cache.valueSize + t.cache.internalNodeCount*node.InternalNodeSize
}
