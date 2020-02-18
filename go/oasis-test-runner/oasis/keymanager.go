package oasis

import (
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/oasislabs/oasis-core/go/common/node"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/crypto"
	registry "github.com/oasislabs/oasis-core/go/registry/api"
)

const (
	kmStatusFile = "keymanager_status.json"
	kmPolicyFile = "keymanager_policy.cbor"

	keymanagerIdentitySeedTemplate = "ekiden node keymanager %d"
)

// Keymanager is an Oasis key manager.
type Keymanager struct { // nolint: maligned
	Node

	sentryIndices []int

	runtime *Runtime
	entity  *Entity

	tmAddress        string
	consensusPort    uint16
	workerClientPort uint16
}

// KeymanagerCfg is the Oasis key manager provisioning configuration.
type KeymanagerCfg struct {
	NodeCfg

	SentryIndices []int

	Runtime *Runtime
	Entity  *Entity
}

// IdentityKeyPath returns the paths to the node's identity key.
func (km *Keymanager) IdentityKeyPath() string {
	return nodeIdentityKeyPath(km.dir)
}

// P2PKeyPath returns the paths to the node's P2P key.
func (km *Keymanager) P2PKeyPath() string {
	return nodeP2PKeyPath(km.dir)
}

// ConsensusKeyPath returns the path to the node's consensus key.
func (km *Keymanager) ConsensusKeyPath() string {
	return nodeConsensusKeyPath(km.dir)
}

// ExportsPath returns the path to the node's exports data dir.
func (km *Keymanager) ExportsPath() string {
	return nodeExportsPath(km.dir)
}

// Start starts an Oasis node.
func (km *Keymanager) Start() error {
	return km.startNode()
}

func (km *Keymanager) provisionGenesis() error {
	// Provision status and policy. We can only provision this here as we need
	// a list of runtimes allowed to query the key manager.
	statusArgs := []string{
		"keymanager", "init_status",
		"--debug.dont_blame_oasis",
		"--debug.allow_test_keys",
		"--keymanager.status.id", km.runtime.id.String(),
		"--keymanager.status.file", filepath.Join(km.dir.String(), kmStatusFile),
	}
	if km.runtime.teeHardware == node.TEEHardwareInvalid {
		// Status without policy.
		statusArgs = append(statusArgs, "--keymanager.policy.file", "")
	} else {
		// Status and policy signed with test keys.
		kmPolicyPath := filepath.Join(km.dir.String(), kmPolicyFile)
		policyArgs := []string{
			"keymanager", "init_policy",
			"--debug.dont_blame_oasis",
			"--keymanager.policy.file", kmPolicyPath,
			"--keymanager.policy.id", km.runtime.id.String(),
			"--keymanager.policy.serial", "1",
			"--keymanager.policy.enclave.id", km.runtime.mrEnclave.String() + km.runtime.mrSigner.String(),
		}

		for _, rt := range km.net.runtimes {
			if rt.teeHardware == node.TEEHardwareInvalid || rt.kind != registry.KindCompute {
				continue
			}

			arg := fmt.Sprintf("%s=%s%s", rt.id, rt.mrEnclave, rt.mrSigner)
			policyArgs = append(policyArgs, "--keymanager.policy.may.query", arg)
		}

		w, err := km.dir.NewLogWriter("provision-policy.log")
		if err != nil {
			return err
		}
		defer w.Close()

		if err = km.net.runNodeBinary(w, policyArgs...); err != nil {
			km.net.logger.Error("failed to provision keymanager policy",
				"err", err,
			)
			return errors.Wrap(err, "oasis/keymanager: failed to provision keymanager policy")
		}

		// Sign policy with test keys.
		signArgsTpl := []string{
			"keymanager", "sign_policy",
			"--debug.allow_test_keys",
			"--debug.dont_blame_oasis",
			"--keymanager.policy.file", kmPolicyPath,
		}
		for i := 1; i <= 3; i++ {
			signatureFile := filepath.Join(km.dir.String(), fmt.Sprintf("%s.sign.%d", kmPolicyFile, i))
			signArgs := append([]string{}, signArgsTpl...)
			signArgs = append(signArgs, []string{
				"--keymanager.policy.signature.file", signatureFile,
				"--keymanager.policy.testkey", fmt.Sprintf("%d", i),
			}...)
			statusArgs = append(statusArgs, "--keymanager.policy.signature.file", signatureFile)

			w, err := km.dir.NewLogWriter("provision-policy-sign.log")
			if err != nil {
				return err
			}
			defer w.Close()

			if err = km.net.runNodeBinary(w, signArgs...); err != nil {
				km.net.logger.Error("failed to sign keymanager policy",
					"err", err,
				)
				return errors.Wrap(err, "oasis/keymanager: failed to sign keymanager policy")
			}
		}

		statusArgs = append(statusArgs, "--keymanager.policy.file", kmPolicyPath)
	}

	w, err := km.dir.NewLogWriter("provision-status.log")
	if err != nil {
		return err
	}
	defer w.Close()

	if err = km.net.runNodeBinary(w, statusArgs...); err != nil {
		km.net.logger.Error("failed to provision keymanager status",
			"err", err,
		)
		return errors.Wrap(err, "oasis/keymanager: failed to provision keymanager status")
	}

	return nil
}

func (km *Keymanager) toGenesisArgs() []string {
	return []string{
		"--keymanager", filepath.Join(km.dir.String(), kmStatusFile),
	}
}

func (km *Keymanager) startNode() error {
	var err error

	sentries, err := resolveSentries(km.net, km.sentryIndices)
	if err != nil {
		return err
	}

	args := newArgBuilder().
		debugDontBlameOasis().
		debugAllowTestKeys().
		tendermintCoreListenAddress(km.consensusPort).
		tendermintSubmissionGasPrice(km.submissionGasPrice).
		workerClientPort(km.workerClientPort).
		workerKeymangerEnabled().
		workerKeymanagerRuntimeBinary(km.runtime.binary).
		workerKeymanagerRuntimeLoader(km.net.cfg.RuntimeLoaderBinary).
		workerKeymanagerRuntimeID(km.runtime.id).
		workerKeymanagerMayGenerate().
		appendNetwork(km.net).
		appendSeedNodes(km.net).
		appendEntity(km.entity)

	if km.runtime.teeHardware != node.TEEHardwareInvalid {
		args = args.workerKeymanagerTEEHardware(km.runtime.teeHardware)
	}

	// Sentry configuration.
	if len(sentries) > 0 {
		args = args.addSentries(sentries).
			tendermintDisablePeerExchange()
	} else {
		args = args.appendSeedNodes(km.net)
	}

	if km.cmd, km.exitCh, err = km.net.startOasisNode(km.dir, nil, args, km.Name, false, km.restartable); err != nil {
		return fmt.Errorf("oasis/keymanager: failed to launch node %s: %w", km.Name, err)
	}

	return nil
}

// NewKeymanger provisions a new keymanager and adds it to the network.
func (net *Network) NewKeymanager(cfg *KeymanagerCfg) (*Keymanager, error) {
	// XXX: Technically there can be more than one keymanager.
	if len(net.keymanagers) == 1 {
		return nil, errors.New("oasis/keymanager: already provisioned")
	}

	kmName := "keymanager"

	kmDir, err := net.baseDir.NewSubDir(kmName)
	if err != nil {
		net.logger.Error("failed to create keymanager subdir",
			"err", err,
		)
		return nil, errors.Wrap(err, "oasis/keymanager: failed to create keymanager subdir")
	}

	// Pre-provision the node identity so that we can update the entity.
	// TODO: Use proper key manager index when multiple key managers are supported.
	seed := fmt.Sprintf(keymanagerIdentitySeedTemplate, 0)
	publicKey, err := net.provisionNodeIdentity(kmDir, seed)
	if err != nil {
		return nil, errors.Wrap(err, "oasis/keymanager: failed to provision node identity")
	}
	if err := cfg.Entity.addNode(publicKey); err != nil {
		return nil, err
	}

	km := &Keymanager{
		Node: Node{
			Name:                                     kmName,
			net:                                      net,
			dir:                                      kmDir,
			restartable:                              cfg.Restartable,
			disableDefaultLogWatcherHandlerFactories: cfg.DisableDefaultLogWatcherHandlerFactories,
			logWatcherHandlerFactories:               cfg.LogWatcherHandlerFactories,
			submissionGasPrice:                       cfg.SubmissionGasPrice,
		},
		runtime:          cfg.Runtime,
		entity:           cfg.Entity,
		sentryIndices:    cfg.SentryIndices,
		tmAddress:        crypto.PublicKeyToTendermint(&publicKey).Address().String(),
		consensusPort:    net.nextNodePort,
		workerClientPort: net.nextNodePort + 1,
	}
	km.doStartNode = km.startNode
	copy(km.NodeID[:], publicKey[:])

	net.keymanagers = append(net.keymanagers, km)
	net.nextNodePort += 2

	if err := net.AddLogWatcher(&km.Node); err != nil {
		net.logger.Error("failed to add log watcher",
			"err", err,
		)
		return nil, fmt.Errorf("oasis/keymanager: failed to add log watcher: %w", err)
	}

	return km, nil
}
