package state

import (
	"context"

	"github.com/oasislabs/oasis-core/go/common"
	"github.com/oasislabs/oasis-core/go/common/cbor"
	"github.com/oasislabs/oasis-core/go/common/keyformat"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/abci"
	"github.com/oasislabs/oasis-core/go/keymanager/api"
	mkvs "github.com/oasislabs/oasis-core/go/storage/mkvs/urkel"
)

var (
	// statusKeyFmt is the key manager status key format.
	//
	// Value is CBOR-serialized key manager status.
	statusKeyFmt = keyformat.New(0x70, &common.Namespace{})
)

type ImmutableState struct {
	*abci.ImmutableState
}

func (st *ImmutableState) Statuses(ctx context.Context) ([]*api.Status, error) {
	rawStatuses, err := st.getStatusesRaw(ctx)
	if err != nil {
		return nil, err
	}

	var statuses []*api.Status
	for _, raw := range rawStatuses {
		var status api.Status
		if err = cbor.Unmarshal(raw, &status); err != nil {
			return nil, abci.UnavailableStateError(err)
		}
		statuses = append(statuses, &status)
	}

	return statuses, nil
}

func (st *ImmutableState) getStatusesRaw(ctx context.Context) ([][]byte, error) {
	it := st.Tree.NewIterator(ctx)
	defer it.Close()

	var rawVec [][]byte
	for it.Seek(statusKeyFmt.Encode()); it.Valid(); it.Next() {
		if !statusKeyFmt.Decode(it.Key()) {
			break
		}
		rawVec = append(rawVec, it.Value())
	}
	if it.Err() != nil {
		return nil, abci.UnavailableStateError(it.Err())
	}
	return rawVec, nil
}

func (st *ImmutableState) Status(ctx context.Context, id common.Namespace) (*api.Status, error) {
	data, err := st.Tree.Get(ctx, statusKeyFmt.Encode(&id))
	if err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	if data == nil {
		return nil, api.ErrNoSuchStatus
	}

	var status api.Status
	if err := cbor.Unmarshal(data, &status); err != nil {
		return nil, abci.UnavailableStateError(err)
	}
	return &status, nil
}

func NewImmutableState(ctx context.Context, state abci.ApplicationState, version int64) (*ImmutableState, error) {
	inner, err := abci.NewImmutableState(ctx, state, version)
	if err != nil {
		return nil, err
	}
	return &ImmutableState{inner}, nil
}

// MutableState is a mutable key manager state wrapper.
type MutableState struct {
	*ImmutableState
}

func (st *MutableState) SetStatus(ctx context.Context, status *api.Status) error {
	err := st.Tree.Insert(ctx, statusKeyFmt.Encode(&status.ID), cbor.Marshal(status))
	return abci.UnavailableStateError(err)
}

// NewMutableState creates a new mutable key manager state wrapper.
func NewMutableState(tree mkvs.KeyValueTree) *MutableState {
	return &MutableState{
		ImmutableState: &ImmutableState{
			&abci.ImmutableState{Tree: tree},
		},
	}
}
