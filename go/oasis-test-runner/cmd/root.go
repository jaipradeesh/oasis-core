// Package cmd implements the commands for the test-runner executable.
package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/oasislabs/oasis-core/go/common/logging"
	"github.com/oasislabs/oasis-core/go/oasis-node/cmd/common"
	cmdFlags "github.com/oasislabs/oasis-core/go/oasis-node/cmd/common/flags"
	"github.com/oasislabs/oasis-core/go/oasis-test-runner/env"
	"github.com/oasislabs/oasis-core/go/oasis-test-runner/oasis"
	"github.com/oasislabs/oasis-core/go/oasis-test-runner/scenario"
)

const (
	cfgConfigFile       = "config"
	cfgLogFmt           = "log.format"
	cfgLogLevel         = "log.level"
	cfgLogNoStdout      = "log.no_stdout"
	cfgTest             = "test"
	cfgParallelJobCount = "parallel.job_count"
	cfgParallelJobIndex = "parallel.job_index"
)

var (
	rootCmd = &cobra.Command{
		Use:     "oasis-test-runner",
		Short:   "Oasis Test Runner",
		Version: "0.0.0-alpha",
		RunE:    runRoot,
	}

	listCmd = &cobra.Command{
		Use:   "list",
		Short: "List registered test cases",
		Run:   runList,
	}

	rootFlags = flag.NewFlagSet("", flag.ContinueOnError)

	cfgFile string

	scenarioMap      = make(map[string]scenario.Scenario)
	defaultScenarios []scenario.Scenario
	scenarios        []scenario.Scenario
	scenarioParameters = make(map[string][]string)
)

// RootCmd returns the root command's structure that will be executed, so that
// it can be used to alter the configuration and flags of the command.
//
// Note: `Run` is pre-initialized to the main entry point of the test harness,
// and should likely be left un-altered.
func RootCmd() *cobra.Command {
	return rootCmd
}

// Execute spawns the main entry point after handing the config file.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		common.EarlyLogAndExit(err)
	}
}

// RegisterNondefault adds a scenario to the runner.
func RegisterNondefault(scenario scenario.Scenario, parameters []string) error {
	n := strings.ToLower(scenario.Name())
	if _, ok := scenarioMap[n]; ok {
		return errors.New("root: scenario already registered: " + n)
	}

	scenarioMap[n] = scenario
	scenarios = append(scenarios, scenario)

	if parameters != nil && len(parameters) > 0 {
		scenarioParameters[n] = parameters
	}

	return nil
}

// Register adds a scenario to the runner and the default scenarios list.
func Register(scenario scenario.Scenario, parameters []string) error {
	if err := RegisterNondefault(scenario, parameters); err != nil {
		return err
	}

	defaultScenarios = append(defaultScenarios, scenario)
	return nil
}

func initRootEnv(cmd *cobra.Command) (*env.Env, error) {
	// Initialize the root dir.
	rootDir := env.GetRootDir()
	if err := rootDir.Init(cmd); err != nil {
		return nil, err
	}
	env := env.New(rootDir)

	var ok bool
	defer func() {
		if !ok {
			env.Cleanup()
		}
	}()

	var logFmt logging.Format
	if err := logFmt.Set(viper.GetString(cfgLogFmt)); err != nil {
		return nil, errors.Wrap(err, "root: failed to set log format")
	}

	var logLevel logging.Level
	if err := logLevel.Set(viper.GetString(cfgLogLevel)); err != nil {
		return nil, errors.Wrap(err, "root: failed to set log level")
	}

	// Initialize logging.
	logFile := filepath.Join(env.Dir(), "test-runner.log")
	w, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "root: failed to open log file")
	}

	var logWriter io.Writer = w
	if !viper.GetBool(cfgLogNoStdout) {
		logWriter = io.MultiWriter(os.Stdout, w)
	}
	if err := logging.Initialize(logWriter, logFmt, logLevel, nil); err != nil {
		return nil, errors.Wrap(err, "root: failed to initialize logging")
	}

	ok = true
	return env, nil
}

func runRoot(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true

	// Initialize the base dir, logging, etc.
	rootEnv, err := initRootEnv(cmd)
	if err != nil {
		return err
	}
	defer rootEnv.Cleanup()
	logger := logging.GetLogger("test-runner")

	// Enumerate the requested test cases.
	toRun := defaultScenarios // Run all default scenarios if not set.
	if vec := viper.GetStringSlice(cfgTest); len(vec) > 0 {
		toRun = nil
		for _, v := range vec {
			n := strings.ToLower(v)
			scenario, ok := scenarioMap[v]
			if !ok {
				logger.Error("unknown test case",
					"test", n,
				)
				return errors.New("root: unknown test case: " + n)
			}
			toRun = append(toRun, scenario)
		}
	}

	excludeMap := make(map[string]bool)
	if excludeEnv := os.Getenv("OASIS_EXCLUDE_E2E"); excludeEnv != "" {
		for _, v := range strings.Split(excludeEnv, ",") {
			excludeMap[strings.ToLower(v)] = true
		}
	}

	// Run the required test scenarios.
	parallelJobCount := viper.GetInt(cfgParallelJobCount)
	parallelJobIndex := viper.GetInt(cfgParallelJobIndex)

	for index, v := range toRun {
		n := v.Name()

		if index%parallelJobCount != parallelJobIndex {
			logger.Info("skipping test case (assigned to different parallel job)",
				"test", n,
			)
			continue
		}

		if excludeMap[strings.ToLower(n)] {
			logger.Info("skipping test case (excluded by environment)",
				"test", n,
			)
			continue
		}

		logger.Info("running test case",
			"test", n,
		)

		childEnv, err := rootEnv.NewChild(n)
		if err != nil {
			logger.Error("failed to setup child environment",
				"err", err,
				"test", n,
			)
			return errors.Wrap(err, "root: failed to setup child environment")
		}

		if err = doScenario(childEnv, v); err != nil {
			logger.Error("failed to run test case",
				"err", err,
				"test", n,
			)
			err = errors.Wrap(err, "root: failed to run test case")
		}

		if cleanErr := doCleanup(childEnv); cleanErr != nil {
			logger.Error("failed to clean up child envionment",
				"err", cleanErr,
				"test", n,
			)
			if err == nil {
				err = errors.Wrap(cleanErr, "root: failed to clean up child enviroment")
			}
		}

		if err != nil {
			return err
		}

		logger.Info("passed test case",
			"test", n,
		)
	}

	return nil
}

func doScenario(childEnv *env.Env, scenario scenario.Scenario) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("root: panic caught running test case: %v", r)
		}
	}()

	var fixture *oasis.NetworkFixture
	if fixture, err = scenario.Fixture(); err != nil {
		err = errors.Wrap(err, "root: failed to initialize network fixture")
		return
	}

	// Instantiate fixture if it is non-nil. Otherwise assume Init will do
	// something on its own.
	var net *oasis.Network
	if fixture != nil {
		if net, err = fixture.Create(childEnv); err != nil {
			err = errors.Wrap(err, "root: failed to instantiate fixture")
			return
		}
	}

	if err = scenario.Init(childEnv, net); err != nil {
		err = errors.Wrap(err, "root: failed to initialize test case")
		return
	}

	if err = scenario.Run(childEnv); err != nil {
		err = errors.Wrap(err, "root: failed to run test case")
		return
	}

	return
}

func doCleanup(childEnv *env.Env) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("root: panic caught cleaning up test case: %v", r)
		}
	}()

	childEnv.Cleanup()

	return
}

func runList(cmd *cobra.Command, args []string) {
	switch len(scenarios) {
	case 0:
		fmt.Printf("No supported test cases!\n")
	default:
		fmt.Printf("Supported test cases:\n")

		// Sort scenarios alphabetically before printing.
		sort.Slice(scenarios, func(i, j int) bool {
			return scenarios[i].Name() < scenarios[j].Name()
		})

		for _, v := range scenarios {
			fmt.Printf("  * %v", v.Name())
			params := scenarioParameters[strings.ToLower(v.Name())]
			if params != nil {
				fmt.Printf(" (parameters:")
				for _, p := range params {
					fmt.Printf(" %v", p)
				}
				fmt.Printf(")")
			}
			fmt.Printf("\n")
		}
	}
}

func init() {
	logFmt := logging.FmtLogfmt
	logLevel := logging.LevelWarn

	// Register flags.
	rootFlags.StringVar(&cfgFile, cfgConfigFile, "", "config file")
	rootFlags.Var(&logFmt, cfgLogFmt, "log format")
	rootFlags.Var(&logLevel, cfgLogLevel, "log level")
	rootFlags.Bool(cfgLogNoStdout, false, "do not mutiplex logs to stdout")
	rootFlags.StringSliceP(cfgTest, "t", nil, "test(s) to run")
	rootFlags.Int(cfgParallelJobCount, 1, "(for CI) number of overall parallel jobs")
	rootFlags.Int(cfgParallelJobIndex, 0, "(for CI) index of this parallel job")
	_ = viper.BindPFlags(rootFlags)

	rootCmd.PersistentFlags().AddFlagSet(rootFlags)
	rootCmd.PersistentFlags().AddFlagSet(env.Flags)
	rootCmd.AddCommand(listCmd)

	cobra.OnInitialize(func() {
		if cfgFile != "" {
			viper.SetConfigFile(cfgFile)
			if err := viper.ReadInConfig(); err != nil {
				common.EarlyLogAndExit(err)
			}
		}

		viper.Set(cmdFlags.CfgDebugDontBlameOasis, true)
	})
}
