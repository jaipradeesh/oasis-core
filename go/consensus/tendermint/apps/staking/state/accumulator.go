package state

import (
	"context"
	"fmt"

	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/quantity"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/abci"
	staking "github.com/oasislabs/oasis-core/go/staking/api"
)

// StakeAccumulatorCache is a thin wrapper around an underlying escrow accounts which caches some
// values (like the threshold map) between operations.
type StakeAccumulatorCache struct {
	ctx context.Context
	// state is the mutable state.
	state *MutableState

	// accounts is a map of staking accounts that we are changing.
	accounts     map[signature.PublicKey]*staking.Account
	accountOrder []signature.PublicKey

	// thresholds is a cache of the threshold map.
	thresholds map[staking.ThresholdKind]quantity.Quantity
}

func (c *StakeAccumulatorCache) getAccount(id signature.PublicKey) (*staking.Account, error) {
	if c.accounts == nil {
		c.accounts = make(map[signature.PublicKey]*staking.Account)
		c.accountOrder = nil
	}

	if a := c.accounts[id]; a != nil {
		return a, nil
	}

	a, err := c.state.Account(c.ctx, id)
	if err != nil {
		return nil, err
	}
	c.accounts[id] = a
	c.accountOrder = append(c.accountOrder, id)
	return a, nil
}

// CheckStakeClaims checks whether the escrow account balance satisfies all the stake claims.
func (c *StakeAccumulatorCache) CheckStakeClaims(id signature.PublicKey) error {
	acct, err := c.getAccount(id)
	if err != nil {
		return err
	}
	return acct.Escrow.CheckStakeClaims(c.thresholds)
}

// AddStakeClaim attempts to add a stake claim to the given escrow account.
//
// In case there is insufficient stake to cover the claim or an error occurrs, no modifications are
// made to the stake accumulator.
func (c *StakeAccumulatorCache) AddStakeClaim(id signature.PublicKey, claim staking.StakeClaim, thresholds []staking.ThresholdKind) error {
	acct, err := c.getAccount(id)
	if err != nil {
		return err
	}
	return acct.Escrow.AddStakeClaim(c.thresholds, claim, thresholds)
}

// RemoveStakeClaim removes a given stake claim.
//
// It is an error if the stake claim does not exist.
func (c *StakeAccumulatorCache) RemoveStakeClaim(id signature.PublicKey, claim staking.StakeClaim) error {
	acct, err := c.getAccount(id)
	if err != nil {
		return err
	}
	return acct.Escrow.RemoveStakeClaim(claim)
}

// GetEscrowBalance returns a given account's escrow balance.
func (c *StakeAccumulatorCache) GetEscrowBalance(id signature.PublicKey) (*quantity.Quantity, error) {
	acct, err := c.getAccount(id)
	if err != nil {
		return nil, err
	}
	return acct.Escrow.Active.Balance.Clone(), nil
}

// Commit commits the stake accumulator changes. The caller must ensure that this does not overwrite
// any outstanding account updates.
func (c *StakeAccumulatorCache) Commit() error {
	for _, id := range c.accountOrder {
		if err := c.state.SetAccount(c.ctx, id, c.accounts[id]); err != nil {
			return fmt.Errorf("failed to set account %s: %w", id, err)
		}
	}
	return nil
}

// Discard discards any stake accumulator changes.
func (c *StakeAccumulatorCache) Discard() {
	c.accounts = nil
	c.accountOrder = nil
}

// NewStakeAccumulatorCache creates a new stake accumulator cache.
func NewStakeAccumulatorCache(ctx *abci.Context) (*StakeAccumulatorCache, error) {
	state := NewMutableState(ctx.State())

	thresholds, err := state.Thresholds(ctx)
	if err != nil {
		return nil, fmt.Errorf("staking/tendermint: failed to query thresholds: %w", err)
	}

	return &StakeAccumulatorCache{
		ctx:        ctx,
		state:      state,
		thresholds: thresholds,
	}, nil
}

// AddStakeClaim is a convenience function for adding a single stake claim to an entity.
//
// In case there is no errors, the added claim is automatically committed. The caller must ensure
// that this does not overwrite any outstanding account updates.
func AddStakeClaim(ctx *abci.Context, id signature.PublicKey, claim staking.StakeClaim, thresholds []staking.ThresholdKind) error {
	sa, err := NewStakeAccumulatorCache(ctx)
	if err != nil {
		return err
	}
	if err = sa.AddStakeClaim(id, claim, thresholds); err != nil {
		return err
	}
	return sa.Commit()
}

// RemoveStakeClaim is a convenience function for removing a single stake claim from an entity.
//
// In case there is no errors, the removed claim is automatically committed. The caller must ensure
// that this does not overwrite any outstanding account updates.
func RemoveStakeClaim(ctx *abci.Context, id signature.PublicKey, claim staking.StakeClaim) error {
	sa, err := NewStakeAccumulatorCache(ctx)
	if err != nil {
		return err
	}
	if err = sa.RemoveStakeClaim(id, claim); err != nil {
		return err
	}
	return sa.Commit()
}

// CheckStakeClaims is a convenience function for checking a single entity's stake claims.
func CheckStakeClaims(ctx *abci.Context, id signature.PublicKey) error {
	sa, err := NewStakeAccumulatorCache(ctx)
	if err != nil {
		return err
	}
	defer sa.Discard()

	return sa.CheckStakeClaims(id)
}
