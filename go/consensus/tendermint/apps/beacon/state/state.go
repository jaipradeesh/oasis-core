package state

import (
	"context"
	"errors"
	"fmt"

	beacon "github.com/oasislabs/oasis-core/go/beacon/api"
	"github.com/oasislabs/oasis-core/go/common/cbor"
	"github.com/oasislabs/oasis-core/go/common/keyformat"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/abci"
	mkvs "github.com/oasislabs/oasis-core/go/storage/mkvs/urkel"
)

var (
	// beaconKeyFmt is the random beacon key format.
	//
	// Value is raw random beacon.
	beaconKeyFmt = keyformat.New(0x40)
	// parametersKeyFmt is the key format used for consensus parameters.
	//
	// Value is CBOR-serialized beacon.ConsensusParameters.
	parametersKeyFmt = keyformat.New(0x41)
)

type ImmutableState struct {
	*abci.ImmutableState
}

func NewImmutableState(ctx context.Context, state abci.ApplicationState, version int64) (*ImmutableState, error) {
	inner, err := abci.NewImmutableState(ctx, state, version)
	if err != nil {
		return nil, err
	}

	return &ImmutableState{inner}, nil
}

// Beacon gets the current random beacon value.
func (s *ImmutableState) Beacon(ctx context.Context) ([]byte, error) {
	data, err := s.Tree.Get(ctx, beaconKeyFmt.Encode())
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if data == nil {
		return nil, beacon.ErrBeaconNotAvailable
	}
	return data, nil
}

func (s *ImmutableState) ConsensusParameters(ctx context.Context) (*beacon.ConsensusParameters, error) {
	data, err := s.Tree.Get(ctx, parametersKeyFmt.Encode())
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if data == nil {
		return nil, errors.New("tendermint/beacon: expected consensus parameters to be present in app state")
	}

	var params beacon.ConsensusParameters
	if err = cbor.Unmarshal(data, &params); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &params, nil
}

// MutableState is a mutable beacon state wrapper.
type MutableState struct {
	*ImmutableState
}

func (s *MutableState) SetBeacon(ctx context.Context, newBeacon []byte) error {
	if l := len(newBeacon); l != beacon.BeaconSize {
		return fmt.Errorf("tendermint/beacon: unexpected beacon size: %d", l)
	}

	err := s.Tree.Insert(ctx, beaconKeyFmt.Encode(), newBeacon)
	return abci.UnavailableStateError(err)
}

func (s *MutableState) SetConsensusParameters(ctx context.Context, params *beacon.ConsensusParameters) error {
	err := s.Tree.Insert(ctx, parametersKeyFmt.Encode(), cbor.Marshal(params))
	return abci.UnavailableStateError(err)
}

// NewMutableState creates a new mutable beacon state wrapper.
func NewMutableState(tree mkvs.KeyValueTree) *MutableState {
	return &MutableState{
		ImmutableState: &ImmutableState{
			&abci.ImmutableState{Tree: tree},
		},
	}
}
