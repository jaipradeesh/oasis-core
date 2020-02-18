package state

import (
	"context"
	"errors"

	"github.com/oasislabs/oasis-core/go/common"
	"github.com/oasislabs/oasis-core/go/common/cbor"
	"github.com/oasislabs/oasis-core/go/common/crypto/hash"
	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/entity"
	"github.com/oasislabs/oasis-core/go/common/keyformat"
	"github.com/oasislabs/oasis-core/go/common/node"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/abci"
	tmcrypto "github.com/oasislabs/oasis-core/go/consensus/tendermint/crypto"
	registry "github.com/oasislabs/oasis-core/go/registry/api"
	mkvs "github.com/oasislabs/oasis-core/go/storage/mkvs/urkel"
)

var (
	_ registry.NodeLookup    = (*ImmutableState)(nil)
	_ registry.RuntimeLookup = (*ImmutableState)(nil)

	// signedEntityKeyFmt is the key format used for signed entities.
	//
	// Value is CBOR-serialized signed entity.
	signedEntityKeyFmt = keyformat.New(0x10, &signature.PublicKey{})
	// signedNodeKeyFmt is the key format used for signed nodes.
	//
	// Value is CBOR-serialized signed node.
	signedNodeKeyFmt = keyformat.New(0x11, &signature.PublicKey{})
	// signedNodeByEntityKeyFmt is the key format used for signed node by entity
	// index.
	//
	// Value is empty.
	signedNodeByEntityKeyFmt = keyformat.New(0x12, &signature.PublicKey{}, &signature.PublicKey{})
	// signedRuntimeKeyFmt is the key format used for signed runtimes.
	//
	// Value is CBOR-serialized signed runtime.
	signedRuntimeKeyFmt = keyformat.New(0x13, &common.Namespace{})
	// nodeByConsAddressKeyFmt is the key format used for the consensus address to
	// node public key mapping.
	//
	// The only reason why this is needed is because Tendermint only gives you
	// the validator address (which is the truncated SHA-256 of the public key) in
	// evidence instead of the actual public key.
	//
	// Value is binary node public key.
	nodeByConsAddressKeyFmt = keyformat.New(0x14, []byte{})
	// nodeStatusKeyFmt is the key format used for node statuses.
	//
	// Value is CBOR-serialized node status.
	nodeStatusKeyFmt = keyformat.New(0x15, &signature.PublicKey{})
	// parametersKeyFmt is the key format used for consensus parameters.
	//
	// Value is CBOR-serialized registry.ConsensusParameters.
	parametersKeyFmt = keyformat.New(0x16)
	// keyMapKeyFmt is the key format used for key-to-node-id map.
	// This stores the consensus and P2P to Node ID mappings.
	//
	// Value is binary signature.PublicKey (node ID).
	keyMapKeyFmt = keyformat.New(0x17, &signature.PublicKey{})
	// certificateMapKeyFmt is the key format used for certificate-to-node-id map.
	// This stores the hash-of-certificate to Node ID mappings.
	//
	// Value is binary signature.PublicKey (node ID).
	certificateMapKeyFmt = keyformat.New(0x18, &hash.Hash{})
	// suspendedRuntimeKeyFmt is the key format used for suspended runtimes.
	//
	// Value is CBOR-serialized signed runtime.
	suspendedRuntimeKeyFmt = keyformat.New(0x19, &common.Namespace{})
	// signedRuntimeByEntityKeyFmt is the key format used for signed runtime by entity
	// index.
	//
	// Value is empty.
	signedRuntimeByEntityKeyFmt = keyformat.New(0x1a, &signature.PublicKey{}, &common.Namespace{})
)

type ImmutableState struct {
	*abci.ImmutableState
}

func (s *ImmutableState) getSignedEntityRaw(ctx context.Context, id signature.PublicKey) ([]byte, error) {
	data, err := s.Tree.Get(ctx, signedEntityKeyFmt.Encode(&id))
	return data, abci.UnavailableStateError(err)
}

// Entity looks up a registered entity by its identifier.
func (s *ImmutableState) Entity(ctx context.Context, id signature.PublicKey) (*entity.Entity, error) {
	signedEntityRaw, err := s.getSignedEntityRaw(ctx, id)
	if err != nil {
		return nil, err
	}
	if signedEntityRaw == nil {
		return nil, registry.ErrNoSuchEntity
	}

	var signedEntity entity.SignedEntity
	if err = cbor.Unmarshal(signedEntityRaw, &signedEntity); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	var entity entity.Entity
	if err = cbor.Unmarshal(signedEntity.Blob, &entity); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &entity, nil
}

// Entities returns a list of all registered entities.
func (s *ImmutableState) Entities(ctx context.Context) ([]*entity.Entity, error) {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	var entities []*entity.Entity
	for it.Seek(signedEntityKeyFmt.Encode()); it.Valid(); it.Next() {
		if !signedEntityKeyFmt.Decode(it.Key()) {
			break
		}

		var signedEntity entity.SignedEntity
		if err := cbor.Unmarshal(it.Value(), &signedEntity); err != nil {
			return nil, abci.UnavailableStateError(err)
		}
		var entity entity.Entity
		if err := cbor.Unmarshal(signedEntity.Blob, &entity); err != nil {
			return nil, abci.UnavailableStateError(err)
		}

		entities = append(entities, &entity)
	}
	if it.Err() != nil {
		return nil, abci.UnavailableStateError(it.Err())
	}
	return entities, nil
}

// SignedEntities returns a list of all registered entities (signed).
func (s *ImmutableState) SignedEntities(ctx context.Context) ([]*entity.SignedEntity, error) {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	var entities []*entity.SignedEntity
	for it.Seek(signedEntityKeyFmt.Encode()); it.Valid(); it.Next() {
		if !signedEntityKeyFmt.Decode(it.Key()) {
			break
		}

		var signedEntity entity.SignedEntity
		if err := cbor.Unmarshal(it.Value(), &signedEntity); err != nil {
			return nil, abci.UnavailableStateError(err)
		}

		entities = append(entities, &signedEntity)
	}
	if it.Err() != nil {
		return nil, abci.UnavailableStateError(it.Err())
	}
	return entities, nil
}

func (s *ImmutableState) getSignedNodeRaw(ctx context.Context, id signature.PublicKey) ([]byte, error) {
	data, err := s.Tree.Get(ctx, signedNodeKeyFmt.Encode(&id))
	return data, abci.UnavailableStateError(err)
}

// Node looks up a specific node by its identifier.
func (s *ImmutableState) Node(ctx context.Context, id signature.PublicKey) (*node.Node, error) {
	signedNodeRaw, err := s.getSignedNodeRaw(ctx, id)
	if err != nil {
		return nil, err
	}
	if signedNodeRaw == nil {
		return nil, registry.ErrNoSuchNode
	}

	var signedNode node.MultiSignedNode
	if err = cbor.Unmarshal(signedNodeRaw, &signedNode); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	var node node.Node
	if err = cbor.Unmarshal(signedNode.Blob, &node); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &node, nil
}

// NodeByConsensusAddress looks up a specific node by its consensus address.
func (s *ImmutableState) NodeByConsensusAddress(ctx context.Context, address []byte) (*node.Node, error) {
	rawID, err := s.Tree.Get(ctx, nodeByConsAddressKeyFmt.Encode(address))
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if rawID == nil {
		return nil, registry.ErrNoSuchNode
	}

	var id signature.PublicKey
	if err := id.UnmarshalBinary(rawID); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return s.Node(ctx, id)
}

// Nodes returns a list of all registered nodes.
func (s *ImmutableState) Nodes(ctx context.Context) ([]*node.Node, error) {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	var nodes []*node.Node
	for it.Seek(signedNodeKeyFmt.Encode()); it.Valid(); it.Next() {
		if !signedNodeKeyFmt.Decode(it.Key()) {
			break
		}

		var signedNode node.MultiSignedNode
		if err := cbor.Unmarshal(it.Value(), &signedNode); err != nil {
			return nil, abci.UnavailableStateError(err)
		}
		var node node.Node
		if err := cbor.Unmarshal(signedNode.Blob, &node); err != nil {
			return nil, abci.UnavailableStateError(err)
		}

		nodes = append(nodes, &node)
	}
	if it.Err() != nil {
		return nil, abci.UnavailableStateError(it.Err())
	}
	return nodes, nil
}

// SignedNodes returns a list of all registered nodes (in signed form).
func (s *ImmutableState) SignedNodes(ctx context.Context) ([]*node.MultiSignedNode, error) {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	var nodes []*node.MultiSignedNode
	for it.Seek(signedNodeKeyFmt.Encode()); it.Valid(); it.Next() {
		if !signedNodeKeyFmt.Decode(it.Key()) {
			break
		}

		var signedNode node.MultiSignedNode
		if err := cbor.Unmarshal(it.Value(), &signedNode); err != nil {
			return nil, abci.UnavailableStateError(err)
		}

		nodes = append(nodes, &signedNode)
	}
	if it.Err() != nil {
		return nil, abci.UnavailableStateError(it.Err())
	}
	return nodes, nil
}

func (s *ImmutableState) getSignedRuntime(ctx context.Context, keyFmt *keyformat.KeyFormat, id common.Namespace) (*registry.SignedRuntime, error) {
	raw, err := s.Tree.Get(ctx, keyFmt.Encode(&id))
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if raw == nil {
		return nil, registry.ErrNoSuchRuntime
	}

	var signedRuntime registry.SignedRuntime
	if err := cbor.Unmarshal(raw, &signedRuntime); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &signedRuntime, nil
}

func (s *ImmutableState) getRuntime(ctx context.Context, keyFmt *keyformat.KeyFormat, id common.Namespace) (*registry.Runtime, error) {
	signedRuntime, err := s.getSignedRuntime(ctx, keyFmt, id)
	if err != nil {
		return nil, err
	}
	var runtime registry.Runtime
	if err = cbor.Unmarshal(signedRuntime.Blob, &runtime); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &runtime, nil
}

// Runtime looks up a runtime by its identifier and returns it.
//
// This excludes any suspended runtimes, use SuspendedRuntime to query
// suspended runtimes only.
func (s *ImmutableState) Runtime(ctx context.Context, id common.Namespace) (*registry.Runtime, error) {
	return s.getRuntime(ctx, signedRuntimeKeyFmt, id)
}

// SuspendedRuntime looks up a suspended runtime by its identifier and
// returns it.
func (s *ImmutableState) SuspendedRuntime(ctx context.Context, id common.Namespace) (*registry.Runtime, error) {
	return s.getRuntime(ctx, suspendedRuntimeKeyFmt, id)
}

// AnyRuntime looks up either an active or suspended runtime by its identifier and returns it.
func (s *ImmutableState) AnyRuntime(ctx context.Context, id common.Namespace) (rt *registry.Runtime, err error) {
	rt, err = s.Runtime(ctx, id)
	if err == registry.ErrNoSuchRuntime {
		rt, err = s.SuspendedRuntime(ctx, id)
	}
	return
}

// SignedRuntime looks up a (signed) runtime by its identifier and returns it.
//
// This excludes any suspended runtimes, use SuspendedSignedRuntime to query
// suspended runtimes only.
func (s *ImmutableState) SignedRuntime(ctx context.Context, id common.Namespace) (*registry.SignedRuntime, error) {
	return s.getSignedRuntime(ctx, signedRuntimeKeyFmt, id)
}

// SignedSuspendedRuntime looks up a (signed) suspended runtime by its identifier and returns it.
func (s *ImmutableState) SignedSuspendedRuntime(ctx context.Context, id common.Namespace) (*registry.SignedRuntime, error) {
	return s.getSignedRuntime(ctx, suspendedRuntimeKeyFmt, id)
}

func (s *ImmutableState) iterateRuntimes(
	ctx context.Context,
	keyFmt *keyformat.KeyFormat,
	cb func(*registry.SignedRuntime) error,
) error {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	for it.Seek(keyFmt.Encode()); it.Valid(); it.Next() {
		if !keyFmt.Decode(it.Key()) {
			break
		}

		var signedRt registry.SignedRuntime
		if err := cbor.Unmarshal(it.Value(), &signedRt); err != nil {
			return abci.UnavailableStateError(err)
		}

		if err := cb(&signedRt); err != nil {
			return err
		}
	}
	return abci.UnavailableStateError(it.Err())
}

// SignedRuntimes returns a list of all registered runtimes (signed).
//
// This excludes any suspended runtimes.
func (s *ImmutableState) SignedRuntimes(ctx context.Context) ([]*registry.SignedRuntime, error) {
	var runtimes []*registry.SignedRuntime
	err := s.iterateRuntimes(ctx, signedRuntimeKeyFmt, func(rt *registry.SignedRuntime) error {
		runtimes = append(runtimes, rt)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return runtimes, nil
}

// SuspendedRuntimes returns a list of all suspended runtimes (signed).
func (s *ImmutableState) SuspendedRuntimes(ctx context.Context) ([]*registry.SignedRuntime, error) {
	var runtimes []*registry.SignedRuntime
	err := s.iterateRuntimes(ctx, suspendedRuntimeKeyFmt, func(rt *registry.SignedRuntime) error {
		runtimes = append(runtimes, rt)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return runtimes, nil
}

// AllSignedRuntimes returns a list of all runtimes (suspended included).
func (s *ImmutableState) AllSignedRuntimes(ctx context.Context) ([]*registry.SignedRuntime, error) {
	var runtimes []*registry.SignedRuntime
	err := s.iterateRuntimes(ctx, signedRuntimeKeyFmt, func(rt *registry.SignedRuntime) error {
		runtimes = append(runtimes, rt)
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = s.iterateRuntimes(ctx, suspendedRuntimeKeyFmt, func(rt *registry.SignedRuntime) error {
		runtimes = append(runtimes, rt)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return runtimes, nil
}

// Runtimes returns a list of all registered runtimes.
//
// This excludes any suspended runtimes.
func (s *ImmutableState) Runtimes(ctx context.Context) ([]*registry.Runtime, error) {
	var runtimes []*registry.Runtime
	err := s.iterateRuntimes(ctx, signedRuntimeKeyFmt, func(sigRt *registry.SignedRuntime) error {
		var rt registry.Runtime
		if err := cbor.Unmarshal(sigRt.Blob, &rt); err != nil {
			return abci.UnavailableStateError(err)
		}
		runtimes = append(runtimes, &rt)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return runtimes, nil
}

// AllRuntimes returns a list of all registered runtimes (suspended included).
func (s *ImmutableState) AllRuntimes(ctx context.Context) ([]*registry.Runtime, error) {
	var runtimes []*registry.Runtime
	unpackFn := func(sigRt *registry.SignedRuntime) error {
		var rt registry.Runtime
		if err := cbor.Unmarshal(sigRt.Blob, &rt); err != nil {
			return abci.UnavailableStateError(err)
		}
		runtimes = append(runtimes, &rt)
		return nil
	}
	if err := s.iterateRuntimes(ctx, signedRuntimeKeyFmt, unpackFn); err != nil {
		return nil, err
	}
	if err := s.iterateRuntimes(ctx, suspendedRuntimeKeyFmt, unpackFn); err != nil {
		return nil, err
	}
	return runtimes, nil
}

// NodeStatus returns a specific node status.
func (s *ImmutableState) NodeStatus(ctx context.Context, id signature.PublicKey) (*registry.NodeStatus, error) {
	value, err := s.Tree.Get(ctx, nodeStatusKeyFmt.Encode(&id))
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if value == nil {
		return nil, registry.ErrNoSuchNode
	}

	var status registry.NodeStatus
	if err := cbor.Unmarshal(value, &status); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &status, nil
}

// NodeStatuses returns all of the node statuses.
func (s *ImmutableState) NodeStatuses(ctx context.Context) (map[signature.PublicKey]*registry.NodeStatus, error) {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	statuses := make(map[signature.PublicKey]*registry.NodeStatus)
	for it.Seek(nodeStatusKeyFmt.Encode()); it.Valid(); it.Next() {
		var nodeID signature.PublicKey
		if !nodeStatusKeyFmt.Decode(it.Key(), &nodeID) {
			break
		}

		var status registry.NodeStatus
		if err := cbor.Unmarshal(it.Value(), &status); err != nil {
			return nil, abci.UnavailableStateError(err)
		}

		statuses[nodeID] = &status
	}
	if it.Err() != nil {
		return nil, abci.UnavailableStateError(it.Err())
	}
	return statuses, nil
}

// HasEntityNodes checks whether an entity has any registered nodes.
func (s *ImmutableState) HasEntityNodes(ctx context.Context, id signature.PublicKey) (bool, error) {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	if it.Seek(signedNodeByEntityKeyFmt.Encode(&id)); it.Valid() {
		var entityID signature.PublicKey
		if !signedNodeByEntityKeyFmt.Decode(it.Key(), &entityID) || !entityID.Equal(id) {
			return false, nil
		}
		return true, nil
	}
	return false, abci.UnavailableStateError(it.Err())
}

// HasEntityRuntimes checks whether an entity has any registered runtimes.
func (s *ImmutableState) HasEntityRuntimes(ctx context.Context, id signature.PublicKey) (bool, error) {
	it := s.Tree.NewIterator(ctx)
	defer it.Close()

	if it.Seek(signedRuntimeByEntityKeyFmt.Encode(&id)); it.Valid() {
		var entityID signature.PublicKey
		if !signedRuntimeByEntityKeyFmt.Decode(it.Key(), &entityID) || !entityID.Equal(id) {
			return false, nil
		}
		return true, nil
	}
	return false, abci.UnavailableStateError(it.Err())
}

// ConsensusParameters returns the registry consensus parameters.
func (s *ImmutableState) ConsensusParameters(ctx context.Context) (*registry.ConsensusParameters, error) {
	raw, err := s.Tree.Get(ctx, parametersKeyFmt.Encode())
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if raw == nil {
		return nil, errors.New("tendermint/registry: expected consensus parameters to be present in app state")
	}

	var params registry.ConsensusParameters
	if err := cbor.Unmarshal(raw, &params); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &params, nil
}

// NodeByConsensusOrP2PKey looks up a specific node by its consensus or P2P key.
func (s *ImmutableState) NodeByConsensusOrP2PKey(ctx context.Context, key signature.PublicKey) (*node.Node, error) {
	rawID, err := s.Tree.Get(ctx, keyMapKeyFmt.Encode(&key))
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if rawID == nil {
		return nil, registry.ErrNoSuchNode
	}

	var id signature.PublicKey
	if err := id.UnmarshalBinary(rawID); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return s.Node(ctx, id)
}

// Hashes a node's committee certificate into a key for the certificate to node ID map.
func nodeCertificateToMapKey(cert []byte) hash.Hash {
	var h hash.Hash
	h.FromBytes(cert)
	return h
}

// NodeByCertificate looks up a specific node by its certificate.
func (s *ImmutableState) NodeByCertificate(ctx context.Context, cert []byte) (*node.Node, error) {
	certHash := nodeCertificateToMapKey(cert)
	rawID, err := s.Tree.Get(ctx, certificateMapKeyFmt.Encode(&certHash))
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if rawID == nil {
		return nil, registry.ErrNoSuchNode
	}

	var id signature.PublicKey
	if err := id.UnmarshalBinary(rawID); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return s.Node(ctx, id)
}

func NewImmutableState(ctx context.Context, state abci.ApplicationState, version int64) (*ImmutableState, error) {
	inner, err := abci.NewImmutableState(ctx, state, version)
	if err != nil {
		return nil, err
	}

	return &ImmutableState{inner}, nil
}

// MutableState is a mutable registry state wrapper.
type MutableState struct {
	*ImmutableState
}

// SetEntity sets a signed entity descriptor for a registered entity.
func (s *MutableState) SetEntity(ctx context.Context, ent *entity.Entity, sigEnt *entity.SignedEntity) error {
	err := s.Tree.Insert(ctx, signedEntityKeyFmt.Encode(&ent.ID), cbor.Marshal(sigEnt))
	return abci.UnavailableStateError(err)
}

// RemoveEntity removes a previously registered entity.
func (s *MutableState) RemoveEntity(ctx context.Context, id signature.PublicKey) (*entity.Entity, error) {
	data, err := s.Tree.RemoveExisting(ctx, signedEntityKeyFmt.Encode(&id))
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if data != nil {
		var removedSignedEntity entity.SignedEntity
		if err = cbor.Unmarshal(data, &removedSignedEntity); err != nil {
			return nil, abci.UnavailableStateError(err)
		}
		var removedEntity entity.Entity
		if err = cbor.Unmarshal(removedSignedEntity.Blob, &removedEntity); err != nil {
			return nil, abci.UnavailableStateError(err)
		}
		return &removedEntity, nil
	}
	return nil, registry.ErrNoSuchEntity
}

// SetNode sets a signed node descriptor for a registered node.
func (s *MutableState) SetNode(ctx context.Context, node *node.Node, signedNode *node.MultiSignedNode) error {
	address := []byte(tmcrypto.PublicKeyToTendermint(&node.Consensus.ID).Address())
	rawNodeID, err := node.ID.MarshalBinary()
	if err != nil {
		return err
	}

	if err = s.Tree.Insert(ctx, signedNodeKeyFmt.Encode(&node.ID), cbor.Marshal(signedNode)); err != nil {
		return abci.UnavailableStateError(err)
	}
	if err = s.Tree.Insert(ctx, signedNodeByEntityKeyFmt.Encode(&node.EntityID, &node.ID), []byte("")); err != nil {
		return abci.UnavailableStateError(err)
	}

	if err = s.Tree.Insert(ctx, nodeByConsAddressKeyFmt.Encode(address), rawNodeID); err != nil {
		return abci.UnavailableStateError(err)
	}

	if err = s.Tree.Insert(ctx, keyMapKeyFmt.Encode(&node.Consensus.ID), rawNodeID); err != nil {
		return abci.UnavailableStateError(err)
	}
	if err = s.Tree.Insert(ctx, keyMapKeyFmt.Encode(&node.P2P.ID), rawNodeID); err != nil {
		return abci.UnavailableStateError(err)
	}

	certHash := nodeCertificateToMapKey(node.Committee.Certificate)
	if err = s.Tree.Insert(ctx, certificateMapKeyFmt.Encode(&certHash), rawNodeID); err != nil {
		return abci.UnavailableStateError(err)
	}

	return nil
}

// RemoveNode removes a registered node.
func (s *MutableState) RemoveNode(ctx context.Context, node *node.Node) error {
	if err := s.Tree.Remove(ctx, signedNodeKeyFmt.Encode(&node.ID)); err != nil {
		return abci.UnavailableStateError(err)
	}
	if err := s.Tree.Remove(ctx, signedNodeByEntityKeyFmt.Encode(&node.EntityID, &node.ID)); err != nil {
		return abci.UnavailableStateError(err)
	}
	if err := s.Tree.Remove(ctx, nodeStatusKeyFmt.Encode(&node.ID)); err != nil {
		return abci.UnavailableStateError(err)
	}

	address := []byte(tmcrypto.PublicKeyToTendermint(&node.Consensus.ID).Address())
	if err := s.Tree.Remove(ctx, nodeByConsAddressKeyFmt.Encode(address)); err != nil {
		return abci.UnavailableStateError(err)
	}

	if err := s.Tree.Remove(ctx, keyMapKeyFmt.Encode(&node.Consensus.ID)); err != nil {
		return abci.UnavailableStateError(err)
	}
	if err := s.Tree.Remove(ctx, keyMapKeyFmt.Encode(&node.P2P.ID)); err != nil {
		return abci.UnavailableStateError(err)
	}

	certHash := nodeCertificateToMapKey(node.Committee.Certificate)
	if err := s.Tree.Remove(ctx, certificateMapKeyFmt.Encode(&certHash)); err != nil {
		return abci.UnavailableStateError(err)
	}

	return nil
}

// SetRuntime sets a signed runtime descriptor for a registered runtime.
func (s *MutableState) SetRuntime(ctx context.Context, rt *registry.Runtime, sigRt *registry.SignedRuntime, suspended bool) error {
	if err := s.Tree.Insert(ctx, signedRuntimeByEntityKeyFmt.Encode(&rt.EntityID, &rt.ID), []byte("")); err != nil {
		return abci.UnavailableStateError(err)
	}

	var err error
	if suspended {
		err = s.Tree.Insert(ctx, suspendedRuntimeKeyFmt.Encode(&rt.ID), cbor.Marshal(sigRt))
	} else {
		err = s.Tree.Insert(ctx, signedRuntimeKeyFmt.Encode(&rt.ID), cbor.Marshal(sigRt))
	}
	return abci.UnavailableStateError(err)
}

// SuspendRuntime marks a runtime as suspended.
func (s *MutableState) SuspendRuntime(ctx context.Context, id common.Namespace) error {
	data, err := s.Tree.RemoveExisting(ctx, signedRuntimeKeyFmt.Encode(&id))
	if err != nil {
		return abci.UnavailableStateError(err)
	}
	if data == nil {
		return registry.ErrNoSuchRuntime
	}
	return s.Tree.Insert(ctx, suspendedRuntimeKeyFmt.Encode(&id), data)
}

// ResumeRuntime resumes a previously suspended runtime.
func (s *MutableState) ResumeRuntime(ctx context.Context, id common.Namespace) error {
	data, err := s.Tree.RemoveExisting(ctx, suspendedRuntimeKeyFmt.Encode(&id))
	if err != nil {
		return abci.UnavailableStateError(err)
	}
	if data == nil {
		return registry.ErrNoSuchRuntime
	}
	return s.Tree.Insert(ctx, signedRuntimeKeyFmt.Encode(&id), data)
}

// SetNodeStatus sets a status for a registered node.
func (s *MutableState) SetNodeStatus(ctx context.Context, id signature.PublicKey, status *registry.NodeStatus) error {
	err := s.Tree.Insert(ctx, nodeStatusKeyFmt.Encode(&id), cbor.Marshal(status))
	return abci.UnavailableStateError(err)
}

// SetConsensusParameters sets registry consensus parameters.
func (s *MutableState) SetConsensusParameters(ctx context.Context, params *registry.ConsensusParameters) error {
	err := s.Tree.Insert(ctx, parametersKeyFmt.Encode(), cbor.Marshal(params))
	return abci.UnavailableStateError(err)
}

// NewMutableState creates a new mutable registry state wrapper.
func NewMutableState(tree mkvs.KeyValueTree) *MutableState {
	return &MutableState{
		ImmutableState: &ImmutableState{
			&abci.ImmutableState{Tree: tree},
		},
	}
}
