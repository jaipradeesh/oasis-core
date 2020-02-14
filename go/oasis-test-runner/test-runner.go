// Oasis network integration test harness.
package main

import (
	"github.com/oasislabs/oasis-core/go/oasis-test-runner/cmd"
	"github.com/oasislabs/oasis-core/go/oasis-test-runner/scenario/e2e"
)

func main() {
	// The general idea is that it should be possible to reuse everything
	// except for the main() function  and specialized test cases to write
	// test drivers that need to exercise the Oasis network or things built
	// up on the Oasis network.
	//
	// Other implementations will likely want to override parts of rootCmd,
	// in particular the `Use`, `Short`, and `Version` fields.
	rootCmd := cmd.RootCmd()

	// Register the e2e test cases.
	rootCmd.Flags().AddFlagSet(e2e.Flags)
	// Basic test.
	_ = cmd.Register(e2e.Basic, e2e.NoParameters)
	_ = cmd.Register(e2e.BasicEncryption, e2e.NoParameters)
	// Byzantine executor node.
	_ = cmd.Register(e2e.ByzantineExecutorHonest, e2e.NoParameters)
	_ = cmd.Register(e2e.ByzantineExecutorWrong, e2e.NoParameters)
	_ = cmd.Register(e2e.ByzantineExecutorStraggler, e2e.NoParameters)
	// Byzantine merge node.
	_ = cmd.Register(e2e.ByzantineMergeHonest, e2e.NoParameters)
	_ = cmd.Register(e2e.ByzantineMergeWrong, e2e.NoParameters)
	_ = cmd.Register(e2e.ByzantineMergeStraggler, e2e.NoParameters)
	// Storage sync test.
	_ = cmd.Register(e2e.StorageSync, e2e.NoParameters)
	// Sentry test.
	_ = cmd.Register(e2e.Sentry, e2e.NoParameters)
	_ = cmd.Register(e2e.SentryEncryption, e2e.NoParameters)
	// Keymanager restart test.
	_ = cmd.Register(e2e.KeymanagerRestart, e2e.NoParameters)
	// Dump/restore test.
	_ = cmd.Register(e2e.DumpRestore, e2e.NoParameters)
	// Halt test.
	_ = cmd.Register(e2e.HaltRestore, e2e.NoParameters)
	// Multiple runtimes test.
	_ = cmd.Register(e2e.MultipleRuntimes, e2e.MultipleRuntimesParameters)
	// Registry CLI test.
	_ = cmd.Register(e2e.RegistryCLI, e2e.NoParameters)
	// Stake CLI test.
	_ = cmd.Register(e2e.StakeCLI, e2e.NoParameters)
	// Node shutdown test.
	_ = cmd.Register(e2e.NodeShutdown, e2e.NoParameters)
	// Gas fees tests.
	_ = cmd.Register(e2e.GasFeesStaking, e2e.NoParameters)
	_ = cmd.Register(e2e.GasFeesRuntimes, e2e.NoParameters)
	// Identity CLI test.
	_ = cmd.Register(e2e.IdentityCLI, e2e.NoParameters)
	// Runtime prune test.
	_ = cmd.Register(e2e.RuntimePrune, e2e.NoParameters)
	// Runtime dynamic registration test.
	_ = cmd.Register(e2e.RuntimeDynamic, e2e.NoParameters)
	// Transaction source test.
	_ = cmd.Register(e2e.TxSourceTransferShort, e2e.NoParameters)
	_ = cmd.RegisterNondefault(e2e.TxSourceTransfer, e2e.NoParameters)
	// Node upgrade tests.
	_ = cmd.Register(e2e.NodeUpgrade, e2e.NoParameters)
	_ = cmd.Register(e2e.NodeUpgradeCancel, e2e.NoParameters)

	// Execute the command, now that everything has been initialized.
	cmd.Execute()
}
