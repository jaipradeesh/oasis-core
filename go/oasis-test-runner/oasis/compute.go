package oasis

import (
	"fmt"

	"github.com/pkg/errors"

	registry "github.com/oasislabs/oasis-core/go/registry/api"
	storageClient "github.com/oasislabs/oasis-core/go/storage/client"
	workerHost "github.com/oasislabs/oasis-core/go/worker/common/host"
)

const (
	computeIdentitySeedTemplate = "ekiden node worker %d"

	ByzantineDefaultIdentitySeed = "ekiden byzantine node worker" // slot 0
	ByzantineSlot1IdentitySeed   = "ekiden byzantine node worker, luck=1"
	ByzantineSlot2IdentitySeed   = "ekiden byzantine node worker, luck=11"
	ByzantineSlot3IdentitySeed   = "ekiden byzantine node worker, luck=6"
)

// Compute is an Oasis compute node.
type Compute struct { // nolint: maligned
	Node

	entity *Entity

	runtimeBackend string

	consensusPort uint16
	clientPort    uint16
	p2pPort       uint16
}

// ComputeCfg is the Oasis compute node configuration.
type ComputeCfg struct {
	NodeCfg

	Entity *Entity

	RuntimeBackend string
}

// IdentityKeyPath returns the path to the node's identity key.
func (worker *Compute) IdentityKeyPath() string {
	return nodeIdentityKeyPath(worker.dir)
}

// P2PKeyPath returns the path to the node's P2P key.
func (worker *Compute) P2PKeyPath() string {
	return nodeP2PKeyPath(worker.dir)
}

// ConsensusKeyPath returns the path to the node's consensus key.
func (worker *Compute) ConsensusKeyPath() string {
	return nodeConsensusKeyPath(worker.dir)
}

// Exports path returns the path to the node's exports data dir.
func (worker *Compute) ExportsPath() string {
	return nodeExportsPath(worker.dir)
}

// Start starts an Oasis node.
func (worker *Compute) Start() error {
	return worker.startNode()
}

func (worker *Compute) startNode() error {
	args := newArgBuilder().
		debugDontBlameOasis().
		debugAllowTestKeys().
		tendermintCoreListenAddress(worker.consensusPort).
		tendermintSubmissionGasPrice(worker.submissionGasPrice).
		storageBackend(storageClient.BackendName).
		workerClientPort(worker.clientPort).
		workerP2pPort(worker.p2pPort).
		workerComputeEnabled().
		workerRuntimeBackend(worker.runtimeBackend).
		workerRuntimeLoader(worker.net.cfg.RuntimeLoaderBinary).
		workerTxnschedulerCheckTxEnabled().
		appendNetwork(worker.net).
		appendSeedNodes(worker.net).
		appendEntity(worker.entity)
	for _, v := range worker.net.runtimes {
		if v.kind != registry.KindCompute {
			continue
		}
		args = args.appendComputeNodeRuntime(v)
	}

	var err error
	if worker.cmd, worker.exitCh, err = worker.net.startOasisNode(
		worker.dir,
		nil,
		args,
		worker.Name,
		false,
		worker.restartable,
	); err != nil {
		return fmt.Errorf("oasis/compute: failed to launch node %s: %w", worker.Name, err)
	}

	return nil
}

// NewCompute provisions a new compute node and adds it to the network.
func (net *Network) NewCompute(cfg *ComputeCfg) (*Compute, error) {
	computeName := fmt.Sprintf("compute-%d", len(net.computeWorkers))

	computeDir, err := net.baseDir.NewSubDir(computeName)
	if err != nil {
		net.logger.Error("failed to create compute subdir",
			"err", err,
			"compute_name", computeName,
		)
		return nil, errors.Wrap(err, "oasis/compute: failed to create compute subdir")
	}

	// Pre-provision the node identity so that we can update the entity.
	seed := fmt.Sprintf(computeIdentitySeedTemplate, len(net.computeWorkers))
	publicKey, err := net.provisionNodeIdentity(computeDir, seed)
	if err != nil {
		return nil, errors.Wrap(err, "oasis/compute: failed to provision node identity")
	}
	if err := cfg.Entity.addNode(publicKey); err != nil {
		return nil, err
	}

	if cfg.RuntimeBackend == "" {
		cfg.RuntimeBackend = workerHost.BackendSandboxed
	}

	worker := &Compute{
		Node: Node{
			Name:                                     computeName,
			net:                                      net,
			dir:                                      computeDir,
			restartable:                              cfg.Restartable,
			disableDefaultLogWatcherHandlerFactories: cfg.DisableDefaultLogWatcherHandlerFactories,
			logWatcherHandlerFactories:               cfg.LogWatcherHandlerFactories,
			submissionGasPrice:                       cfg.SubmissionGasPrice,
		},
		entity:         cfg.Entity,
		runtimeBackend: cfg.RuntimeBackend,
		consensusPort:  net.nextNodePort,
		clientPort:     net.nextNodePort + 1,
		p2pPort:        net.nextNodePort + 2,
	}
	worker.doStartNode = worker.startNode
	copy(worker.NodeID[:], publicKey[:])

	net.computeWorkers = append(net.computeWorkers, worker)
	net.nextNodePort += 3

	if err := net.AddLogWatcher(&worker.Node); err != nil {
		net.logger.Error("failed to add log watcher",
			"err", err,
			"compute_name", computeName,
		)
		return nil, fmt.Errorf("oasis/compute: failed to add log watcher for %s: %w", computeName, err)
	}

	return worker, nil
}
