package abci

import (
	"context"
	"fmt"
	"strings"

	"github.com/oasislabs/oasis-core/go/common"
	"github.com/oasislabs/oasis-core/go/common/logging"
	nodedb "github.com/oasislabs/oasis-core/go/storage/mkvs/urkel/db/api"
)

const (
	// PruneDefault is the default PruneStrategy.
	PruneDefault = pruneNone

	pruneNone  = "none"
	pruneKeepN = "keep_n"
)

// PruneStrategy is the strategy to use when pruning the ABCI mux iAVL
// state.
type PruneStrategy int

const (
	// PruneNone retains all versions.
	PruneNone PruneStrategy = iota

	// PruneKeepN retains the last N latest versions.
	PruneKeepN
)

func (s PruneStrategy) String() string {
	switch s {
	case PruneNone:
		return pruneNone
	case PruneKeepN:
		return pruneKeepN
	default:
		return "[unknown]"
	}
}

func (s *PruneStrategy) FromString(str string) error {
	switch strings.ToLower(str) {
	case pruneNone:
		*s = PruneNone
	case pruneKeepN:
		*s = PruneKeepN
	default:
		return fmt.Errorf("abci/pruner: unknown pruning strategy: '%v'", str)
	}

	return nil
}

// PruneConfig is the pruning strategy and related configuration.
type PruneConfig struct {
	// Strategy is the PruneStrategy used.
	Strategy PruneStrategy

	// NumKept is the number of versions retained when applicable.
	NumKept uint64
}

// StatePruner is a concrete ABCI mux state pruner implementation.
type StatePruner interface {
	// Prune purges unneeded versions from the ABCI mux node database,
	// given the latest version, based on the underlying strategy.
	Prune(ctx context.Context, latestRound uint64) error
}

type statePrunerInitializer interface {
	Initialize(latestRound uint64) error
}

type nonePruner struct{}

func (p *nonePruner) Prune(ctx context.Context, latestRound uint64) error {
	// Nothing to prune.
	return nil
}

type genericPruner struct {
	logger *logging.Logger
	ndb    nodedb.NodeDB

	earliestRound uint64
	keepN         uint64
}

func (p *genericPruner) Initialize(latestRound uint64) error {
	// Figure out the eldest version currently present in the tree.
	var err error
	if p.earliestRound, err = p.ndb.GetEarliestRound(context.Background()); err != nil {
		return fmt.Errorf("failed to get earliest round: %w", err)
	}

	return p.doPrune(context.Background(), latestRound)
}

func (p *genericPruner) Prune(ctx context.Context, latestRound uint64) error {
	if err := p.doPrune(ctx, latestRound); err != nil {
		p.logger.Error("Prune",
			"err", err,
		)
		return err
	}
	return nil
}

func (p *genericPruner) doPrune(ctx context.Context, latestRound uint64) error {
	if latestRound < p.keepN {
		return nil
	}

	p.logger.Debug("Prune: Start",
		"latest_version", latestRound,
		"start_version", p.earliestRound,
	)

	preserveFrom := latestRound - p.keepN
	for i := p.earliestRound; i <= latestRound; i++ {
		if i >= preserveFrom {
			p.earliestRound = i
			break
		}

		p.logger.Debug("Prune: Delete",
			"latest_version", latestRound,
			"pruned_version", i,
		)

		if _, err := p.ndb.Prune(ctx, common.Namespace{}, i); err != nil {
			return err
		}
	}

	p.logger.Debug("Prune: Finish",
		"latest_version", latestRound,
		"eldest_version", p.earliestRound,
	)

	return nil
}

func newStatePruner(cfg *PruneConfig, ndb nodedb.NodeDB, latestRound uint64) (StatePruner, error) {
	// The roothash checkCommittees call requires at least 1 previous block
	// for timekeeping purposes.
	const minKept = 1

	logger := logging.GetLogger("abci-mux/pruner")

	var statePruner StatePruner
	switch cfg.Strategy {
	case PruneNone:
		statePruner = &nonePruner{}
	case PruneKeepN:
		if cfg.NumKept < minKept {
			return nil, fmt.Errorf("abci/pruner: invalid number of versions retained: %v", cfg.NumKept)
		}

		statePruner = &genericPruner{
			logger: logger,
			ndb:    ndb,
			keepN:  cfg.NumKept,
		}
	default:
		return nil, fmt.Errorf("abci/pruner: unsupported pruning strategy: %v", cfg.Strategy)
	}

	if initializer, ok := statePruner.(statePrunerInitializer); ok {
		if err := initializer.Initialize(latestRound); err != nil {
			return nil, err
		}
	}

	logger.Debug("ABCI state pruner initialized",
		"strategy", cfg.Strategy,
		"num_kept", cfg.NumKept,
	)

	return statePruner, nil
}
