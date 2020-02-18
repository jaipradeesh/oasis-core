package scheduler

import (
	"context"

	consensus "github.com/oasislabs/oasis-core/go/consensus/api"
	schedulerState "github.com/oasislabs/oasis-core/go/consensus/tendermint/apps/scheduler/state"
	scheduler "github.com/oasislabs/oasis-core/go/scheduler/api"
)

// Query is the scheduler query interface.
type Query interface {
	Validators(context.Context) ([]*scheduler.Validator, error)
	AllCommittees(context.Context) ([]*scheduler.Committee, error)
	KindsCommittees(context.Context, []scheduler.CommitteeKind) ([]*scheduler.Committee, error)
	Genesis(context.Context) (*scheduler.Genesis, error)
}

// QueryFactory is the scheduler query factory.
type QueryFactory struct {
	app *schedulerApplication
}

// QueryAt returns the scheduler query interface for a specific height.
func (sf *QueryFactory) QueryAt(ctx context.Context, height int64) (Query, error) {
	state, err := schedulerState.NewImmutableState(ctx, sf.app.state, height)
	if err != nil {
		return nil, err
	}
	return &schedulerQuerier{state}, nil
}

type schedulerQuerier struct {
	state *schedulerState.ImmutableState
}

func (sq *schedulerQuerier) Validators(ctx context.Context) ([]*scheduler.Validator, error) {
	valPks, err := sq.state.CurrentValidators(ctx)
	if err != nil {
		return nil, err
	}

	// Since we use flat voting power for now, doing it this way saves
	// having to store consensus.VotingPower repeatedly in the validator set
	// ABCI state.
	ret := make([]*scheduler.Validator, 0, len(valPks))
	for _, v := range valPks {
		ret = append(ret, &scheduler.Validator{
			ID:          v,
			VotingPower: consensus.VotingPower,
		})
	}

	return ret, nil
}

func (sq *schedulerQuerier) AllCommittees(ctx context.Context) ([]*scheduler.Committee, error) {
	return sq.state.AllCommittees(ctx)
}

func (sq *schedulerQuerier) KindsCommittees(ctx context.Context, kinds []scheduler.CommitteeKind) ([]*scheduler.Committee, error) {
	return sq.state.KindsCommittees(ctx, kinds)
}

func (app *schedulerApplication) QueryFactory() interface{} {
	return &QueryFactory{app}
}
