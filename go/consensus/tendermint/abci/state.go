package abci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/eapache/channels"
	"github.com/tendermint/tendermint/abci/types"

	"github.com/oasislabs/oasis-core/go/common"
	"github.com/oasislabs/oasis-core/go/common/crypto/hash"
	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/logging"
	"github.com/oasislabs/oasis-core/go/common/quantity"
	consensus "github.com/oasislabs/oasis-core/go/consensus/api"
	"github.com/oasislabs/oasis-core/go/consensus/api/transaction"
	epochtime "github.com/oasislabs/oasis-core/go/epochtime/api"
	genesis "github.com/oasislabs/oasis-core/go/genesis/api"
	storage "github.com/oasislabs/oasis-core/go/storage/api"
	storageDB "github.com/oasislabs/oasis-core/go/storage/database"
	mkvs "github.com/oasislabs/oasis-core/go/storage/mkvs/urkel"
)

var (
	// ErrNoState is the error returned when state is nil.
	ErrNoState = errors.New("tendermint: no state available (app not registered?)")

	_ ApplicationState = (*applicationState)(nil)
	_ ApplicationState = (*mockApplicationState)(nil)
)

// appStateDir is the subdirectory which contains ABCI state.
const appStateDir = "abci-state"

// ApplicationState is the overall past, present and future state of all multiplexed applications.
type ApplicationState interface {
	// Storage returns the storage backend.
	Storage() storage.LocalBackend

	// BlockHeight returns the last committed block height.
	BlockHeight() int64

	// BlockHash returns the last committed block hash.
	BlockHash() []byte

	// BlockContext returns the current block context which can be used
	// to store intermediate per-block results.
	//
	// This method must only be called from BeginBlock/DeliverTx/EndBlock
	// and calls from anywhere else will cause races.
	BlockContext() *BlockContext

	// GetBaseEpoch returns the base epoch.
	GetBaseEpoch() (epochtime.EpochTime, error)

	// GetEpoch returns epoch at block height.
	GetEpoch(ctx context.Context, blockHeight int64) (epochtime.EpochTime, error)

	// GetCurrentEpoch returns the epoch at the current block height.
	GetCurrentEpoch(ctx context.Context) (epochtime.EpochTime, error)

	// EpochChanged returns true iff the current epoch has changed since the
	// last block.  As a matter of convenience, the current epoch is returned.
	EpochChanged(ctx *Context) (bool, epochtime.EpochTime)

	// MinGasPrice returns the configured minimum gas price.
	MinGasPrice() *quantity.Quantity

	// OwnTxSigner returns the transaction signer identity of the local node.
	OwnTxSigner() signature.PublicKey

	// NewContext creates a new application processing context.
	NewContext(mode ContextMode, now time.Time) *Context
}

type applicationState struct {
	logger *logging.Logger

	ctx       context.Context
	cancelCtx context.CancelFunc

	stateRoot     storage.Root
	storage       storage.LocalBackend
	deliverTxTree mkvs.Tree
	checkTxTree   mkvs.Tree

	statePruner    StatePruner
	prunerClosedCh chan struct{}
	prunerNotifyCh *channels.RingChannel

	blockLock sync.RWMutex
	blockTime time.Time
	blockCtx  *BlockContext

	txAuthHandler TransactionAuthHandler

	timeSource epochtime.Backend

	haltMode        bool
	haltEpochHeight epochtime.EpochTime

	minGasPrice quantity.Quantity
	ownTxSigner signature.PublicKey

	metricsClosedCh chan struct{}
}

func (s *applicationState) NewContext(mode ContextMode, now time.Time) *Context {
	s.blockLock.RLock()
	defer s.blockLock.RUnlock()

	c := &Context{
		mode:          mode,
		currentTime:   now,
		gasAccountant: NewNopGasAccountant(),
		appState:      s,
		blockHeight:   int64(s.stateRoot.Round),
		logger:        logging.GetLogger("consensus/tendermint/abci").With("mode", mode),
	}
	c.Context = context.WithValue(s.ctx, contextKey{}, c)

	switch mode {
	case ContextInitChain:
		c.state = s.deliverTxTree
	case ContextCheckTx:
		c.state = s.checkTxTree
	case ContextDeliverTx, ContextBeginBlock, ContextEndBlock:
		c.state = s.deliverTxTree
		c.blockCtx = s.blockCtx
	case ContextSimulateTx:
		// Since simulation is running in parallel to any changes to the database, we make sure
		// to create a separate in-memory tree at the given block height.
		c.state = mkvs.NewWithRoot(nil, s.storage.NodeDB(), s.stateRoot, mkvs.WithoutWriteLog())
		c.currentTime = s.blockTime
	default:
		panic(fmt.Errorf("context: invalid mode: %s (%d)", mode, mode))
	}

	return c
}

func (s *applicationState) Storage() storage.LocalBackend {
	return s.storage
}

// BlockHeight returns the last committed block height.
func (s *applicationState) BlockHeight() int64 {
	s.blockLock.RLock()
	defer s.blockLock.RUnlock()

	return int64(s.stateRoot.Round)
}

// BlockHash returns the last committed block hash.
func (s *applicationState) BlockHash() []byte {
	s.blockLock.RLock()
	defer s.blockLock.RUnlock()

	if s.stateRoot.Round == 0 {
		// Tendermint expects a nil hash when there is no state otherwise it will panic.
		return nil
	}
	return s.stateRoot.Hash[:]
}

// BlockContext returns the current block context which can be used
// to store intermediate per-block results.
//
// This method must only be called from BeginBlock/DeliverTx/EndBlock
// and calls from anywhere else will cause races.
func (s *applicationState) BlockContext() *BlockContext {
	return s.blockCtx
}

// GetBaseEpoch returns the base epoch.
func (s *applicationState) GetBaseEpoch() (epochtime.EpochTime, error) {
	return s.timeSource.GetBaseEpoch(s.ctx)
}

// GetEpoch returns epoch at block height.
func (s *applicationState) GetEpoch(ctx context.Context, blockHeight int64) (epochtime.EpochTime, error) {
	return s.timeSource.GetEpoch(ctx, blockHeight)
}

// GetCurrentEpoch returns the epoch at the current block height.
func (s *applicationState) GetCurrentEpoch(ctx context.Context) (epochtime.EpochTime, error) {
	blockHeight := s.BlockHeight()
	if blockHeight == 0 {
		return epochtime.EpochInvalid, nil
	}
	currentEpoch, err := s.timeSource.GetEpoch(ctx, blockHeight+1)
	if err != nil {
		return epochtime.EpochInvalid, fmt.Errorf("application state time source get epoch for height %d: %w", blockHeight+1, err)
	}
	return currentEpoch, nil
}

// EpochChanged returns true iff the current epoch has changed since the
// last block.  As a matter of convenience, the current epoch is returned.
func (s *applicationState) EpochChanged(ctx *Context) (bool, epochtime.EpochTime) {
	blockHeight := s.BlockHeight()
	if blockHeight == 0 {
		return false, epochtime.EpochInvalid
	}

	currentEpoch, err := s.timeSource.GetEpoch(ctx, blockHeight+1)
	if err != nil {
		s.logger.Error("EpochChanged: failed to get current epoch",
			"err", err,
		)
		return false, epochtime.EpochInvalid
	}

	if blockHeight == 1 {
		// There is no block before the first block. For historic reasons, this is defined as not
		// having had a transition.
		return false, currentEpoch
	}

	previousEpoch, err := s.timeSource.GetEpoch(ctx, blockHeight)
	if err != nil {
		s.logger.Error("EpochChanged: failed to get previous epoch",
			"err", err,
		)
		return false, epochtime.EpochInvalid
	}

	if previousEpoch == currentEpoch {
		return false, currentEpoch
	}

	s.logger.Debug("EpochChanged: epoch transition detected",
		"prev_epoch", previousEpoch,
		"epoch", currentEpoch,
	)

	return true, currentEpoch
}

// MinGasPrice returns the configured minimum gas price.
func (s *applicationState) MinGasPrice() *quantity.Quantity {
	return &s.minGasPrice
}

// OwnTxSigner returns the transaction signer identity of the local node.
func (s *applicationState) OwnTxSigner() signature.PublicKey {
	return s.ownTxSigner
}

func (s *applicationState) inHaltEpoch(ctx *Context) bool {
	blockHeight := s.BlockHeight()

	currentEpoch, err := s.GetEpoch(ctx, blockHeight+1)
	if err != nil {
		s.logger.Error("inHaltEpoch: failed to get epoch",
			"err", err,
			"block_height", blockHeight+1,
		)
		return false
	}
	s.haltMode = currentEpoch == s.haltEpochHeight
	return s.haltMode
}

func (s *applicationState) afterHaltEpoch(ctx *Context) bool {
	blockHeight := s.BlockHeight()

	currentEpoch, err := s.GetEpoch(ctx, blockHeight+1)
	if err != nil {
		s.logger.Error("afterHaltEpoch: failed to get epoch",
			"err", err,
			"block_height", blockHeight,
		)
		return false
	}

	return currentEpoch > s.haltEpochHeight
}

func (s *applicationState) doCommit(now time.Time) error {
	s.blockLock.Lock()
	defer s.blockLock.Unlock()

	_, stateRootHash, err := s.deliverTxTree.Commit(s.ctx, s.stateRoot.Namespace, s.stateRoot.Round+1)
	if err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	// NOTE: Finalization could be done in parallel, together with pruning since replay into
	//       non-finalized rounds is possible.
	if err = s.storage.NodeDB().Finalize(s.ctx, s.stateRoot.Namespace, s.stateRoot.Round+1, []hash.Hash{stateRootHash}); err != nil {
		return fmt.Errorf("failed to finalize round %d: %w", s.stateRoot.Round+1, err)
	}

	s.stateRoot.Hash = stateRootHash
	s.stateRoot.Round++
	s.blockTime = now

	// Switch the CheckTx tree to the newly committed version. Note that this is safe as Tendermint
	// holds the mempool lock while commit is in progress so no CheckTx can take place.
	s.checkTxTree.Close()
	s.checkTxTree = mkvs.NewWithRoot(nil, s.storage.NodeDB(), s.stateRoot, mkvs.WithoutWriteLog())

	// Notify pruner of a new block.
	s.prunerNotifyCh.In() <- s.stateRoot.Round

	return nil
}

func (s *applicationState) doCleanup() {
	if s.storage != nil {
		// Don't close the DB out from under the metrics/pruner worker.
		s.cancelCtx()
		<-s.prunerClosedCh
		<-s.metricsClosedCh

		s.storage.Cleanup()
		s.storage = nil
	}
}

func (s *applicationState) updateMetrics() error {
	var dbSize int64
	var err error
	if dbSize, err = s.storage.NodeDB().Size(); err != nil {
		s.logger.Error("Size",
			"err", err,
		)
		return err
	}

	abciSize.Set(float64(dbSize) / 1024768.0)

	return nil
}

func (s *applicationState) metricsWorker() {
	defer close(s.metricsClosedCh)

	// Update the metrics once on initialization.
	if err := s.updateMetrics(); err != nil {
		// If this fails, don't bother trying again, it's most likely
		// an unsupported DB backend.
		s.logger.Warn("metrics not available",
			"err", err,
		)
		return
	}

	t := time.NewTicker(metricsUpdateInterval)
	defer t.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			_ = s.updateMetrics()
		}
	}
}

func (s *applicationState) pruneWorker() {
	defer close(s.prunerClosedCh)

	for {
		select {
		case <-s.ctx.Done():
			return
		case r := <-s.prunerNotifyCh.Out():
			round := r.(uint64)

			if err := s.statePruner.Prune(s.ctx, round); err != nil {
				s.logger.Warn("failed to prune state",
					"err", err,
					"block_height", round,
				)
			}
		}
	}
}

func newApplicationState(ctx context.Context, cfg *ApplicationConfig) (*applicationState, error) {
	baseDir := filepath.Join(cfg.DataDir, appStateDir)
	if err := common.Mkdir(baseDir); err != nil {
		return nil, fmt.Errorf("failed to create application state directory: %w", err)
	}

	switch cfg.StorageBackend {
	case storageDB.BackendNameBadgerDB:
	default:
		return nil, fmt.Errorf("unsupported storage backend: %s", cfg.StorageBackend)
	}

	db, err := storageDB.New(&storage.Config{
		Backend:          cfg.StorageBackend,
		DB:               filepath.Join(baseDir, storageDB.DefaultFileName(cfg.StorageBackend)),
		MaxCacheSize:     64 * 1024 * 1024, // TODO: Make this configurable.
		DiscardWriteLogs: true,
		NoFsync:          true, // This is safe as Tendermint will replay on crash.
	})
	if err != nil {
		return nil, err
	}
	ldb := db.(storage.LocalBackend)
	ndb := ldb.NodeDB()

	// Make sure to close the database in case we fail.
	var ok bool
	defer func() {
		if !ok {
			db.Cleanup()
		}
	}()

	// Figure out the latest round/hash if any, and use that as the block height/hash.
	latestRound, err := ndb.GetLatestRound(ctx)
	if err != nil {
		return nil, err
	}
	roots, err := ndb.GetRootsForRound(ctx, latestRound)
	if err != nil {
		return nil, err
	}
	stateRoot := storage.Root{
		Round: latestRound,
	}
	switch len(roots) {
	case 0:
		// No roots -- empty database.
		if latestRound != 0 {
			return nil, fmt.Errorf("state: no roots at non-zero height, corrupted database?")
		}
		stateRoot.Hash.Empty()
	case 1:
		// Exactly one root -- the usual case.
		stateRoot.Hash = roots[0]
	default:
		// More roots -- should not happen for our use case.
		return nil, fmt.Errorf("state: more than one root, corrupted database?")
	}

	// Use the node database directly to avoid going through the syncer interface.
	deliverTxTree := mkvs.NewWithRoot(nil, ndb, stateRoot, mkvs.WithoutWriteLog())
	checkTxTree := mkvs.NewWithRoot(nil, ndb, stateRoot, mkvs.WithoutWriteLog())

	// Initialize the state pruner.
	statePruner, err := newStatePruner(&cfg.Pruning, ndb, latestRound)
	if err != nil {
		return nil, fmt.Errorf("state: failed to create pruner: %w", err)
	}

	var minGasPrice quantity.Quantity
	if err = minGasPrice.FromInt64(int64(cfg.MinGasPrice)); err != nil {
		return nil, fmt.Errorf("state: invalid minimum gas price: %w", err)
	}

	ctx, cancelCtx := context.WithCancel(ctx)

	ok = true

	s := &applicationState{
		logger:          logging.GetLogger("abci-mux/state"),
		ctx:             ctx,
		cancelCtx:       cancelCtx,
		deliverTxTree:   deliverTxTree,
		checkTxTree:     checkTxTree,
		stateRoot:       stateRoot,
		storage:         ldb,
		statePruner:     statePruner,
		prunerClosedCh:  make(chan struct{}),
		prunerNotifyCh:  channels.NewRingChannel(1),
		haltEpochHeight: cfg.HaltEpochHeight,
		minGasPrice:     minGasPrice,
		ownTxSigner:     cfg.OwnTxSigner,
		metricsClosedCh: make(chan struct{}),
	}
	go s.metricsWorker()
	go s.pruneWorker()

	return s, nil
}

func parseGenesisAppState(req types.RequestInitChain) (*genesis.Document, error) {
	var st genesis.Document
	if err := json.Unmarshal(req.AppStateBytes, &st); err != nil {
		return nil, err
	}

	return &st, nil
}

// MockApplicationStateConfig is the configuration for the mock application state.
type MockApplicationStateConfig struct {
	BlockHeight int64
	BlockHash   []byte

	BaseEpoch    epochtime.EpochTime
	CurrentEpoch epochtime.EpochTime
	EpochChanged bool

	MaxBlockGas transaction.Gas
	MinGasPrice *quantity.Quantity

	OwnTxSigner signature.PublicKey
}

type mockApplicationState struct {
	cfg MockApplicationStateConfig

	blockCtx *BlockContext
	tree     mkvs.Tree
}

func (ms *mockApplicationState) Storage() storage.LocalBackend {
	panic("not implemented")
}

func (ms *mockApplicationState) BlockHeight() int64 {
	return ms.cfg.BlockHeight
}

func (ms *mockApplicationState) BlockHash() []byte {
	return ms.cfg.BlockHash
}

func (ms *mockApplicationState) BlockContext() *BlockContext {
	return ms.blockCtx
}

func (ms *mockApplicationState) GetBaseEpoch() (epochtime.EpochTime, error) {
	return ms.cfg.BaseEpoch, nil
}

func (ms *mockApplicationState) GetEpoch(ctx context.Context, blockHeight int64) (epochtime.EpochTime, error) {
	return ms.cfg.CurrentEpoch, nil
}

func (ms *mockApplicationState) GetCurrentEpoch(ctx context.Context) (epochtime.EpochTime, error) {
	return ms.cfg.CurrentEpoch, nil
}

func (ms *mockApplicationState) EpochChanged(ctx *Context) (bool, epochtime.EpochTime) {
	return ms.cfg.EpochChanged, ms.cfg.CurrentEpoch
}

func (ms *mockApplicationState) MinGasPrice() *quantity.Quantity {
	return ms.cfg.MinGasPrice
}

func (ms *mockApplicationState) OwnTxSigner() signature.PublicKey {
	return ms.cfg.OwnTxSigner
}

func (ms *mockApplicationState) NewContext(mode ContextMode, now time.Time) *Context {
	c := &Context{
		mode:          mode,
		currentTime:   now,
		gasAccountant: NewNopGasAccountant(),
		state:         ms.tree,
		appState:      ms,
		blockHeight:   ms.cfg.BlockHeight,
		blockCtx:      ms.blockCtx,
		logger:        logging.GetLogger("consensus/tendermint/abci").With("mode", mode),
	}
	c.Context = context.WithValue(context.Background(), contextKey{}, c)

	return c
}

// NewMockApplicationState creates a new mock application state for testing.
func NewMockApplicationState(cfg MockApplicationStateConfig) ApplicationState {
	tree := mkvs.New(nil, nil)

	blockCtx := NewBlockContext()
	if cfg.MaxBlockGas > 0 {
		blockCtx.Set(GasAccountantKey{}, NewGasAccountant(cfg.MaxBlockGas))
	} else {
		blockCtx.Set(GasAccountantKey{}, NewNopGasAccountant())
	}

	return &mockApplicationState{
		cfg:      cfg,
		blockCtx: blockCtx,
		tree:     tree,
	}
}

// ImmutableState is an immutable state wrapper.
type ImmutableState struct {
	Tree mkvs.KeyValueTree
}

// Close releases the resources associated with the immutable state wrapper.
//
// After calling this method, the immutable state wrapper should not be used anymore.
func (s *ImmutableState) Close() {
	if tree, ok := s.Tree.(mkvs.ClosableTree); ok {
		tree.Close()
	}
}

// NewImmutableState creates a new immutable state wrapper.
func NewImmutableState(ctx context.Context, state ApplicationState, version int64) (*ImmutableState, error) {
	if state == nil {
		return nil, ErrNoState
	}

	// Check if this request was made from an ABCI application context.
	if abciCtx := FromCtx(ctx); abciCtx != nil {
		// Override used state with the one from the current context in the following cases:
		//
		// - If this request was made from InitChain, no blocks and states have been submitted yet.
		// - If this request was made from an ABCI app and is for the current (future) height.
		//
		if abciCtx.IsInitChain() || version == abciCtx.BlockHeight()+1 {
			return &ImmutableState{Tree: abciCtx.State()}, nil
		}
	}

	// Handle a regular (external) query where we need to create a new tree.
	if state.BlockHeight() == 0 {
		return nil, consensus.ErrNoCommittedBlocks
	}
	if version <= 0 || version > state.BlockHeight() {
		version = state.BlockHeight()
	}

	ndb := state.Storage().NodeDB()
	roots, err := ndb.GetRootsForRound(ctx, uint64(version))
	if err != nil {
		return nil, err
	}
	if len(roots) != 1 {
		panic(fmt.Sprintf("corrupted state (%d): %+v", version, roots))
	}
	tree := mkvs.NewWithRoot(nil, ndb, storage.Root{
		Round: uint64(version),
		Hash:  roots[0],
	}, mkvs.WithoutWriteLog())

	return &ImmutableState{Tree: tree}, nil
}
