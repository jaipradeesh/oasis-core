package oasis

import (
	"fmt"

	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	fileSigner "github.com/oasislabs/oasis-core/go/common/crypto/signature/signers/file"
	"github.com/oasislabs/oasis-core/go/common/identity"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/crypto"
)

// Sentry is an Oasis sentry node.
type Sentry struct {
	Node

	validatorIndices  []int
	storageIndices    []int
	keymanagerIndices []int

	publicKey     signature.PublicKey
	tmAddress     string
	consensusPort uint16
	controlPort   uint16
	sentryPort    uint16
}

// SentryCfg is the Oasis sentry node configuration.
type SentryCfg struct {
	NodeCfg

	ValidatorIndices  []int
	StorageIndices    []int
	KeymanagerIndices []int
}

func (sentry *Sentry) startNode() error {
	validators, err := resolveValidators(sentry.net, sentry.validatorIndices)
	if err != nil {
		return err
	}

	storageWorkers, err := resolveStorageWorkers(sentry.net, sentry.storageIndices)
	if err != nil {
		return err
	}

	keymanagerWorkers, err := resolveKeymanagerWorkers(sentry.net, sentry.keymanagerIndices)
	if err != nil {
		return err
	}

	args := newArgBuilder().
		debugDontBlameOasis().
		debugAllowTestKeys().
		workerSentryEnabled().
		workerSentryControlPort(sentry.controlPort).
		tendermintCoreListenAddress(sentry.consensusPort).
		appendNetwork(sentry.net).
		appendSeedNodes(sentry.net)

	if len(validators) > 0 {
		args = args.addValidatorsAsSentryUpstreams(validators)
	}

	if len(storageWorkers) > 0 || len(keymanagerWorkers) > 0 {
		args = args.workerGrpcSentryEnabled().
			workerSentryGrpcClientAddress([]string{fmt.Sprintf("127.0.0.1:%d", sentry.sentryPort)}).
			workerSentryGrpcClientPort(sentry.sentryPort)
	}

	if len(storageWorkers) > 0 {
		args = args.addSentryStorageWorkers(storageWorkers)
	}

	if len(keymanagerWorkers) > 0 {
		args = args.addSentryKeymanagerWorkers(keymanagerWorkers)
	}

	if sentry.cmd, _, err = sentry.net.startOasisNode(sentry.dir, nil, args, sentry.Name, false, false); err != nil {
		return fmt.Errorf("oasis/sentry: failed to launch node %s: %w", sentry.Name, err)
	}

	return nil
}

// NewSentry provisions a new sentry node and adds it to the network.
func (net *Network) NewSentry(cfg *SentryCfg) (*Sentry, error) {
	sentryName := fmt.Sprintf("sentry-%d", len(net.sentries))

	sentryDir, err := net.baseDir.NewSubDir(sentryName)
	if err != nil {
		net.logger.Error("failed to create sentry subdir",
			"err", err,
			"sentry_name", sentryName,
		)
		return nil, fmt.Errorf("oasis/sentry: failed to create sentry subdir: %w", err)
	}

	// Pre-provision node's identity to pass the sentry node's consensus
	// address to the validator so it can configure the sentry node's consensus
	// address as its consensus address.
	signerFactory := fileSigner.NewFactory(sentryDir.String(), signature.SignerNode, signature.SignerP2P, signature.SignerConsensus)
	sentryIdentity, err := identity.LoadOrGenerate(sentryDir.String(), signerFactory)
	if err != nil {
		net.logger.Error("failed to provision sentry identity",
			"err", err,
			"sentry_name", sentryName,
		)
		return nil, fmt.Errorf("oasis/sentry: failed to provision sentry identity: %w", err)
	}
	sentryPublicKey := sentryIdentity.NodeSigner.Public()

	sentry := &Sentry{
		Node: Node{
			Name:                                     sentryName,
			net:                                      net,
			dir:                                      sentryDir,
			disableDefaultLogWatcherHandlerFactories: cfg.DisableDefaultLogWatcherHandlerFactories,
			logWatcherHandlerFactories:               cfg.LogWatcherHandlerFactories,
		},
		validatorIndices:  cfg.ValidatorIndices,
		storageIndices:    cfg.StorageIndices,
		keymanagerIndices: cfg.KeymanagerIndices,
		publicKey:         sentryPublicKey,
		tmAddress:         crypto.PublicKeyToTendermint(&sentryPublicKey).Address().String(),
		consensusPort:     net.nextNodePort,
		controlPort:       net.nextNodePort + 1,
		sentryPort:        net.nextNodePort + 2,
	}
	sentry.doStartNode = sentry.startNode

	net.sentries = append(net.sentries, sentry)
	net.nextNodePort += 3

	if err := net.AddLogWatcher(&sentry.Node); err != nil {
		net.logger.Error("failed to add log watcher",
			"err", err,
			"sentry_name", sentryName,
		)
		return nil, fmt.Errorf("oasis/sentry: failed to add log watcher for %s: %w", sentryName, err)
	}

	return sentry, nil
}
