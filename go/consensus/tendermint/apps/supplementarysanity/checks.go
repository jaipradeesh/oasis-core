package supplementarysanity

import (
	"fmt"

	"github.com/oasislabs/oasis-core/go/common"
	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/quantity"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/abci"
	keymanagerState "github.com/oasislabs/oasis-core/go/consensus/tendermint/apps/keymanager/state"
	registryState "github.com/oasislabs/oasis-core/go/consensus/tendermint/apps/registry/state"
	roothashState "github.com/oasislabs/oasis-core/go/consensus/tendermint/apps/roothash/state"
	stakingState "github.com/oasislabs/oasis-core/go/consensus/tendermint/apps/staking/state"
	epochtime "github.com/oasislabs/oasis-core/go/epochtime/api"
	keymanager "github.com/oasislabs/oasis-core/go/keymanager/api"
	registry "github.com/oasislabs/oasis-core/go/registry/api"
	roothash "github.com/oasislabs/oasis-core/go/roothash/api"
	"github.com/oasislabs/oasis-core/go/roothash/api/block"
	staking "github.com/oasislabs/oasis-core/go/staking/api"
)

func checkEpochTime(ctx *abci.Context, now epochtime.EpochTime) error {
	if now == epochtime.EpochInvalid {
		return fmt.Errorf("current epoch is invalid")
	}

	// nothing to check yet
	return nil
}

func checkRegistry(ctx *abci.Context, now epochtime.EpochTime) error {
	st := registryState.NewMutableState(ctx.State())

	// Check entities.
	entities, err := st.SignedEntities(ctx)
	if err != nil {
		return fmt.Errorf("SignedEntities: %w", err)
	}
	seenEntities, err := registry.SanityCheckEntities(logger, entities)
	if err != nil {
		return fmt.Errorf("SanityCheckEntities: %w", err)
	}

	// Check runtimes.
	runtimes, err := st.SignedRuntimes(ctx)
	if err != nil {
		return fmt.Errorf("AllSignedRuntimes: %w", err)
	}
	suspendedRuntimes, err := st.SuspendedRuntimes(ctx)
	if err != nil {
		return fmt.Errorf("SuspendedRuntimes: %w", err)
	}
	params, err := st.ConsensusParameters(ctx)
	if err != nil {
		return fmt.Errorf("ConsensusParameters: %w", err)
	}
	runtimeLookup, err := registry.SanityCheckRuntimes(logger, params, runtimes, suspendedRuntimes, false)
	if err != nil {
		return fmt.Errorf("SanityCheckRuntimes: %w", err)
	}

	// Check nodes.
	nodes, err := st.SignedNodes(ctx)
	if err != nil {
		return fmt.Errorf("SignedNodes: %w", err)
	}
	err = registry.SanityCheckNodes(logger, params, nodes, seenEntities, runtimeLookup, false, now)
	if err != nil {
		return fmt.Errorf("SanityCheckNodes: %w", err)
	}

	return nil
}

func checkRootHash(ctx *abci.Context, now epochtime.EpochTime) error {
	st := roothashState.NewMutableState(ctx.State())

	// Check blocks.
	runtimes, err := st.Runtimes(ctx)
	if err != nil {
		return fmt.Errorf("Runtimes: %w", err)
	}

	blocks := make(map[common.Namespace]*block.Block)
	for _, rt := range runtimes {
		blocks[rt.Runtime.ID] = rt.CurrentBlock
	}
	err = roothash.SanityCheckBlocks(blocks)
	if err != nil {
		return fmt.Errorf("SanityCheckBlocks: %w", err)
	}

	// nothing to check yet
	return nil
}

func checkStaking(ctx *abci.Context, now epochtime.EpochTime) error {
	st := stakingState.NewMutableState(ctx.State())

	parameters, err := st.ConsensusParameters(ctx)
	if err != nil {
		return fmt.Errorf("ConsensusParameters: %w", err)
	}

	totalSupply, err := st.TotalSupply(ctx)
	if err != nil {
		return fmt.Errorf("TotalSupply: %w", err)
	}
	if !totalSupply.IsValid() {
		return fmt.Errorf("total supply %v is invalid", totalSupply)
	}

	commonPool, err := st.CommonPool(ctx)
	if err != nil {
		return fmt.Errorf("CommonPool: %w", err)
	}
	if !commonPool.IsValid() {
		return fmt.Errorf("common pool %v is invalid", commonPool)
	}

	// Check if the total supply adds up (common pool + all balances in the ledger).
	// Check all commission schedules.
	var total quantity.Quantity
	accounts, err := st.Accounts(ctx)
	if err != nil {
		return fmt.Errorf("Accounts: %w", err)
	}
	var acct *staking.Account
	for _, id := range accounts {
		acct, err = st.Account(ctx, id)
		if err != nil {
			return fmt.Errorf("Account: %w", err)
		}
		err = staking.SanityCheckAccount(&total, parameters, now, id, acct)
		if err != nil {
			return fmt.Errorf("SanityCheckAccount %s: %w", id, err)
		}
	}

	totalFees, err := st.LastBlockFees(ctx)
	if err != nil {
		return fmt.Errorf("LastBlockFees: %w", err)
	}
	if !totalFees.IsValid() {
		return fmt.Errorf("common pool %v is invalid", commonPool)
	}

	_ = total.Add(commonPool)
	_ = total.Add(totalFees)
	if total.Cmp(totalSupply) != 0 {
		return fmt.Errorf("balances in accounts plus common pool (%s) does not add up to total supply (%s)", total.String(), totalSupply.String())
	}

	// All shares of all delegations for a given account must add up to account's Escrow.Active.TotalShares.
	delegationses, err := st.Delegations(ctx)
	if err != nil {
		return fmt.Errorf("Delegations: %w", err)
	}
	for id, delegations := range delegationses {
		acct, err = st.Account(ctx, id)
		if err != nil {
			return fmt.Errorf("Account: %w", err)
		}
		err = staking.SanityCheckDelegations(acct, delegations)
		if err != nil {
			return fmt.Errorf("SanityCheckDelegations %s: %w", id, err)
		}
	}

	// All shares of all debonding delegations for a given account must add up to account's Escrow.Debonding.TotalShares.
	debondingDelegationses, err := st.DebondingDelegations(ctx)
	if err != nil {
		return fmt.Errorf("DebondingDelegations: %w", err)
	}
	for id, debondingDelegations := range debondingDelegationses {
		acct, err = st.Account(ctx, id)
		if err != nil {
			return fmt.Errorf("Account: %w", err)
		}
		err = staking.SanityCheckDebondingDelegations(acct, debondingDelegations)
		if err != nil {
			return fmt.Errorf("SanityCheckDebondingDelegations %s: %w", id, err)
		}
	}

	// Check the above two invariants for each account as well.
	for _, id := range accounts {
		acct, err = st.Account(ctx, id)
		if err != nil {
			return fmt.Errorf("Account: %w", err)
		}
		err = staking.SanityCheckAccountShares(acct, delegationses[id], debondingDelegationses[id])
		if err != nil {
			return fmt.Errorf("SanityCheckAccountShares %s: %w", id, err)
		}
	}

	return nil
}

func checkKeyManager(ctx *abci.Context, now epochtime.EpochTime) error {
	st := keymanagerState.NewMutableState(ctx.State())

	statuses, err := st.Statuses(ctx)
	if err != nil {
		return fmt.Errorf("Statuses: %w", err)
	}
	err = keymanager.SanityCheckStatuses(statuses)
	if err != nil {
		return fmt.Errorf("SanityCheckStatuses: %w", err)
	}

	return nil
}

func checkScheduler(*abci.Context, epochtime.EpochTime) error {
	// nothing to check yet
	return nil
}

func checkBeacon(*abci.Context, epochtime.EpochTime) error {
	// nothing to check yet
	return nil
}

func checkConsensus(*abci.Context, epochtime.EpochTime) error {
	// nothing to check yet
	return nil
}

func checkHalt(*abci.Context, epochtime.EpochTime) error {
	// nothing to check yet
	return nil
}

func checkStakeClaims(ctx *abci.Context, now epochtime.EpochTime) error {
	regSt := registryState.NewMutableState(ctx.State())
	stakeSt := stakingState.NewMutableState(ctx.State())

	params, err := regSt.ConsensusParameters(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consensus parameters: %w", err)
	}

	// Skip checks if stake is being bypassed.
	if params.DebugBypassStake {
		return nil
	}

	// Claims in the stake accumulators should be consistent with general state.
	claims := make(map[signature.PublicKey]map[staking.StakeClaim][]staking.ThresholdKind)
	// Entity registrations.
	entities, err := regSt.Entities(ctx)
	if err != nil {
		return fmt.Errorf("failed to get entities: %w", err)
	}
	for _, entity := range entities {
		claims[entity.ID] = map[staking.StakeClaim][]staking.ThresholdKind{
			registry.StakeClaimRegisterEntity: []staking.ThresholdKind{staking.KindEntity},
		}
	}
	// Node registrations.
	nodes, err := regSt.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get node registrations: %w", err)
	}
	for _, node := range nodes {
		claims[node.EntityID][registry.StakeClaimForNode(node.ID)] = registry.StakeThresholdsForNode(node)
	}
	// Runtime registrations.
	runtimes, err := regSt.AllRuntimes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get runtime registrations: %w", err)
	}
	for _, rt := range runtimes {
		claims[rt.EntityID][registry.StakeClaimForRuntime(rt.ID)] = registry.StakeThresholdsForRuntime(rt)
	}

	// Compare with actual accumulator state.
	for _, entity := range entities {
		acct, err := stakeSt.Account(ctx, entity.ID)
		if err != nil {
			return fmt.Errorf("failed to fetch account: %w", err)
		}
		expectedClaims := claims[entity.ID]
		actualClaims := acct.Escrow.StakeAccumulator.Claims
		if len(expectedClaims) != len(actualClaims) {
			return fmt.Errorf("incorrect number of stake claims for account %s (expected: %d got: %d)",
				entity.ID,
				len(expectedClaims),
				len(actualClaims),
			)
		}
		for claim, expectedThresholds := range expectedClaims {
			thresholds, ok := actualClaims[claim]
			if !ok {
				return fmt.Errorf("missing claim %s for account %s", claim, entity.ID)
			}
			if len(thresholds) != len(expectedThresholds) {
				return fmt.Errorf("incorrect number of thresholds for claim %s for account %s (expected: %d got: %d)",
					claim,
					entity.ID,
					len(expectedThresholds),
					len(thresholds),
				)
			}
			for i, expectedThreshold := range expectedThresholds {
				threshold := thresholds[i]
				if threshold != expectedThreshold {
					return fmt.Errorf("incorrect threshold in position %d for claim %s for account %s (expected: %s got: %s)",
						i,
						claim,
						entity.ID,
						expectedThreshold,
						threshold,
					)
				}
			}
		}
	}

	return nil
}
