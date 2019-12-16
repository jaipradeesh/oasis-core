package committee

import (
	"context"
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/identity"
	"github.com/oasislabs/oasis-core/go/common/logging"
	consensus "github.com/oasislabs/oasis-core/go/consensus/api"
	keymanagerApi "github.com/oasislabs/oasis-core/go/keymanager/api"
	keymanagerClient "github.com/oasislabs/oasis-core/go/keymanager/client"
	registry "github.com/oasislabs/oasis-core/go/registry/api"
	roothash "github.com/oasislabs/oasis-core/go/roothash/api"
	"github.com/oasislabs/oasis-core/go/roothash/api/block"
	scheduler "github.com/oasislabs/oasis-core/go/scheduler/api"
	storage "github.com/oasislabs/oasis-core/go/storage/api"
	"github.com/oasislabs/oasis-core/go/worker/common/host"
	"github.com/oasislabs/oasis-core/go/worker/common/p2p"
)

var (
	processedBlockCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oasis_worker_processed_block_count",
			Help: "Number of processed roothash blocks",
		},
		[]string{"runtime"},
	)
	processedEventCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oasis_worker_processed_event_count",
			Help: "Number of processed roothash events",
		},
		[]string{"runtime"},
	)
	failedRoundCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oasis_worker_failed_round_count",
			Help: "Number of failed roothash rounds",
		},
		[]string{"runtime"},
	)
	epochTransitionCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oasis_worker_epoch_transition_count",
			Help: "Number of epoch transitions",
		},
		[]string{"runtime"},
	)
	nodeCollectors = []prometheus.Collector{
		processedBlockCount,
		processedEventCount,
		failedRoundCount,
		epochTransitionCount,
	}

	metricsOnce sync.Once
)

// NodeHooks defines a worker's duties at common events.
// These are called from the runtime's common node's worker.
type NodeHooks interface {
	HandlePeerMessage(context.Context, *p2p.Message) (bool, error)
	// Guarded by CrossNode.
	HandleEpochTransitionLocked(*EpochSnapshot)
	// Guarded by CrossNode.
	HandleNewBlockEarlyLocked(*block.Block)
	// Guarded by CrossNode.
	HandleNewBlockLocked(*block.Block)
	// Guarded by CrossNode.
	HandleNewEventLocked(*roothash.Event)
}

// Node is a committee node.
type Node struct {
	RuntimeID signature.PublicKey
	Runtime   *registry.Runtime

	Identity         *identity.Identity
	KeyManager       keymanagerApi.Backend
	KeyManagerClient *keymanagerClient.Client
	LocalStorage     *host.LocalStorage
	Storage          storage.Backend
	Roothash         roothash.Backend
	Registry         registry.Backend
	Scheduler        scheduler.Backend
	Consensus        consensus.Backend

	ctx       context.Context
	cancelCtx context.CancelFunc
	stopCh    chan struct{}
	stopOnce  sync.Once
	quitCh    chan struct{}
	initCh    chan struct{}

	Group *Group

	hooks []NodeHooks

	// Mutable and shared between nodes' workers.
	// Guarded by .CrossNode.
	CrossNode    sync.Mutex
	CurrentBlock *block.Block

	logger *logging.Logger
}

// Name returns the service name.
func (n *Node) Name() string {
	return "committee node"
}

// Start starts the service.
func (n *Node) Start() error {
	go n.worker()
	return nil
}

// Stop halts the service.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

// Quit returns a channel that will be closed when the service terminates.
func (n *Node) Quit() <-chan struct{} {
	return n.quitCh
}

// Cleanup performs the service specific post-termination cleanup.
func (n *Node) Cleanup() {
}

// Initialized returns a channel that will be closed when the node is
// initialized and ready to service requests.
func (n *Node) Initialized() <-chan struct{} {
	return n.initCh
}

// AddHooks adds a NodeHooks to be called.
// There is no going back.
func (n *Node) AddHooks(hooks NodeHooks) {
	n.hooks = append(n.hooks, hooks)
}

func (n *Node) getMetricLabels() prometheus.Labels {
	return prometheus.Labels{
		"runtime": n.RuntimeID.String(),
	}
}

// HandlePeerMessage forwards a message from the group system to our hooks.
func (n *Node) HandlePeerMessage(ctx context.Context, message *p2p.Message) error {
	for _, hooks := range n.hooks {
		handled, err := hooks.HandlePeerMessage(ctx, message)
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
	}
	return errors.New("unknown message type")
}

// Guarded by n.CrossNode.
func (n *Node) handleEpochTransitionLocked(height int64) {
	n.logger.Info("epoch transition has occurred")

	epochTransitionCount.With(n.getMetricLabels()).Inc()

	// Transition group.
	if err := n.Group.EpochTransition(n.ctx, height); err != nil {
		n.logger.Error("unable to handle epoch transition",
			"err", err,
		)
	}

	epoch := n.Group.GetEpochSnapshot()
	for _, hooks := range n.hooks {
		hooks.HandleEpochTransitionLocked(epoch)
	}
}

// Guarded by n.CrossNode.
func (n *Node) handleNewBlockLocked(blk *block.Block, height int64) {
	processedBlockCount.With(n.getMetricLabels()).Inc()

	header := blk.Header

	// The first received block will be treated an epoch transition (if valid).
	// This will refresh the committee on the first block,
	// instead of waiting for the next epoch transition to occur.
	// Helps in cases where node is restarted mid epoch.
	firstBlockReceived := n.CurrentBlock == nil

	// Update the current block.
	n.CurrentBlock = blk

	for _, hooks := range n.hooks {
		hooks.HandleNewBlockEarlyLocked(blk)
	}

	// Perform actions based on block type.
	switch header.HeaderType {
	case block.Normal:
		if firstBlockReceived {
			n.logger.Warn("forcing an epoch transition on first received block")
			n.handleEpochTransitionLocked(height)
		} else {
			// Normal block.
			n.Group.RoundTransition(n.ctx)
		}
	case block.RoundFailed:
		if firstBlockReceived {
			n.logger.Warn("forcing an epoch transition on first received block")
			n.handleEpochTransitionLocked(height)
		} else {
			// Round has failed.
			n.logger.Warn("round has failed")
			n.Group.RoundTransition(n.ctx)

			failedRoundCount.With(n.getMetricLabels()).Inc()
		}
	case block.EpochTransition:
		// Process an epoch transition.
		n.handleEpochTransitionLocked(height)
	default:
		n.logger.Error("invalid block type",
			"block", blk,
		)
		return
	}

	for _, hooks := range n.hooks {
		hooks.HandleNewBlockLocked(blk)
	}
}

// Guarded by n.CrossNode.
func (n *Node) handleNewEventLocked(ev *roothash.Event) {
	processedEventCount.With(n.getMetricLabels()).Inc()

	for _, hooks := range n.hooks {
		hooks.HandleNewEventLocked(ev)
	}
}

// waitForRuntime waits for the following:
// - that the consensus service has finished initial synchronization,
// - that the runtime for which this worker is running has been registered
//   in the registry.
func (n *Node) waitForRuntime() error {
	// Delay starting of committee node until after the consensus service
	// has finished initial synchronization, if applicable.
	if n.Consensus != nil {
		n.logger.Info("delaying committee node start until after initial synchronization")
		select {
		case <-n.quitCh:
			return context.Canceled
		case <-n.Consensus.Synced():
		}
	}

	// Wait for the runtime to be registered.
	n.logger.Info("delaying committee node start until the runtime is registered")
	ch, sub, err := n.Registry.WatchRuntimes(n.ctx)
	if err != nil {
		n.logger.Error("failed to watch runtimes",
			"err", err,
		)
		return err
	}
	defer sub.Close()
	for {
		select {
		case <-n.stopCh:
			return context.Canceled
		case rt := <-ch:
			if rt.ID.Equal(n.RuntimeID) {
				n.logger.Info("runtime is registered")

				// Save runtime descriptor.
				n.Runtime = rt
				return nil
			}
		}
	}
}

func (n *Node) worker() {
	n.logger.Info("starting committee node")

	defer close(n.quitCh)
	defer (n.cancelCtx)()

	// Wait for the runtime.
	if err := n.waitForRuntime(); err != nil {
		return
	}

	// Start watching roothash blocks.
	blocks, blocksSub, err := n.Roothash.WatchBlocks(n.RuntimeID)
	if err != nil {
		n.logger.Error("failed to subscribe to roothash blocks",
			"err", err,
		)
		return
	}
	defer blocksSub.Close()

	// Start watching roothash events.
	events, eventsSub, err := n.Roothash.WatchEvents(n.RuntimeID)
	if err != nil {
		n.logger.Error("failed to subscribe to roothash events",
			"err", err,
		)
		return
	}
	defer eventsSub.Close()

	// We are initialized.
	close(n.initCh)

	for {
		select {
		case <-n.stopCh:
			n.logger.Info("termination requested")
			return
		case blk := <-blocks:
			// Received a block (annotated).
			func() {
				n.CrossNode.Lock()
				defer n.CrossNode.Unlock()
				n.handleNewBlockLocked(blk.Block, blk.Height)
			}()
		case ev := <-events:
			// Received an event.
			func() {
				n.CrossNode.Lock()
				defer n.CrossNode.Unlock()
				n.handleNewEventLocked(ev)
			}()
		}
	}
}

func NewNode(
	runtimeID signature.PublicKey,
	identity *identity.Identity,
	keymanager keymanagerApi.Backend,
	keymanagerClient *keymanagerClient.Client,
	localStorage *host.LocalStorage,
	storage storage.Backend,
	roothash roothash.Backend,
	registry registry.Backend,
	scheduler scheduler.Backend,
	consensus consensus.Backend,
	p2p *p2p.P2P,
) (*Node, error) {
	metricsOnce.Do(func() {
		prometheus.MustRegister(nodeCollectors...)
	})

	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		RuntimeID:        runtimeID,
		Identity:         identity,
		KeyManager:       keymanager,
		KeyManagerClient: keymanagerClient,
		LocalStorage:     localStorage,
		Storage:          storage,
		Roothash:         roothash,
		Registry:         registry,
		Scheduler:        scheduler,
		Consensus:        consensus,
		ctx:              ctx,
		cancelCtx:        cancel,
		stopCh:           make(chan struct{}),
		quitCh:           make(chan struct{}),
		initCh:           make(chan struct{}),
		logger:           logging.GetLogger("worker/common/committee").With("runtime_id", runtimeID),
	}

	group, err := NewGroup(identity, runtimeID, n, registry, roothash, scheduler, p2p)
	if err != nil {
		return nil, err
	}
	n.Group = group

	return n, nil
}
