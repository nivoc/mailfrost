package main

import (
	"flag"
	"fmt"
	"mail-backup/internal"
	"os"
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	defaultConfigPath := "config"
	defaultEnvPath := ".env"
	configPath := flag.String("config", defaultConfigPath, "Path to the non-secret config file")
	envPath := flag.String("env", defaultEnvPath, "Path to the .env secrets file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: mail-backup [flags] [command]

Commands:
  backup      Sync mail, audit stable messages, and create a kopia backup
  recover     Restore a snapshot and rewrite the managed IMAP mailboxes to match it
  recover-resume
             Retry the last recovery mbsync push without clearing mailboxes again
  rebaseline  Accept the current Maildir state as the new known-good baseline
  restore     Restore a Maildir snapshot from kopia
  setup       Interactive setup wizard for mbsync and kopia

Flags:
`)
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Restore flags (use after "restore"):
  -snapshot string
        Snapshot ID to restore (skip interactive selection)
  -target string
        Restore target directory (default: ./restored/<snapshot-id>)
  -force
        Allow restoring directly into the configured MAILDIR_PATH without interactive confirmation

Recover flags (use after "recover"):
  -snapshot string
        Snapshot ID to recover (skip interactive selection)
  -yes
        Allow destructive recovery without yes/no prompt
  -confirm-user string
        IMAP username confirmation for non-interactive destructive recovery

Recover-resume flags (use after "recover-resume"):
  -run string
        Recovery run ID to resume (default: latest recovery mbsync config in state dir)
`)
	}
	flag.Parse()

	subcommand := ""
	if args := flag.Args(); len(args) > 0 {
		subcommand = args[0]
	}

	switch subcommand {
	case "":
		flag.Usage()
		return 0
	case "backup":
		return runBackup(*configPath, *envPath)
	case "recover":
		return runRecover(*configPath, *envPath, flag.Args()[1:])
	case "recover-resume":
		return runRecoverResume(*configPath, *envPath, flag.Args()[1:])
	case "rebaseline":
		return runRebaseline(*configPath, *envPath)
	case "restore":
		return runRestore(*configPath, *envPath, flag.Args()[1:])
	case "setup":
		return runSetup(*envPath)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\nUsage: mail-backup [backup|recover|recover-resume|rebaseline|restore|setup]\n", subcommand)
		return 1
	}
}

func runBackup(configPath, envPath string) int {
	config, err := internal.LoadConfig(configPath, envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}

	runtime, err := internal.StartRuntime(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	defer runtime.Close()

	app := &internal.App{Config: config, Runtime: runtime}
	exitCode, err := app.RunBackup()
	if err != nil {
		internal.NotifyRuntimeFailure(runtime, err)
		return 1
	}
	return exitCode
}

func runRebaseline(configPath, envPath string) int {
	config, err := internal.LoadConfig(configPath, envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}

	runtime, err := internal.StartRuntime(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	defer runtime.Close()

	app := &internal.App{Config: config, Runtime: runtime}
	if err := app.RunRebaseline(); err != nil {
		internal.NotifyRuntimeFailure(runtime, err)
		return 1
	}
	return 0
}

func runSetup(envPath string) int {
	app := &internal.SetupApp{EnvPath: envPath}
	if err := app.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	return 0
}

func runRestore(configPath, envPath string, args []string) int {
	restoreFlags := flag.NewFlagSet("restore", flag.ExitOnError)
	snapshotFlag := restoreFlags.String("snapshot", "", "Snapshot ID to restore")
	targetFlag := restoreFlags.String("target", "", "Restore target directory")
	forceFlag := restoreFlags.Bool("force", false, "Allow restoring into the configured MAILDIR_PATH")
	if err := restoreFlags.Parse(args); err != nil {
		return 1
	}

	config, err := internal.LoadConfig(configPath, envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}

	runtime, err := internal.StartRuntime(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	defer runtime.Close()

	app := &internal.RestoreApp{
		Config:       config,
		Runtime:      runtime,
		SnapshotFlag: *snapshotFlag,
		TargetFlag:   *targetFlag,
		ForceFlag:    *forceFlag,
	}
	if err := app.Run(); err != nil {
		internal.NotifyRuntimeFailure(runtime, err)
		return 1
	}
	return 0
}

func runRecover(configPath, envPath string, args []string) int {
	recoverFlags := flag.NewFlagSet("recover", flag.ExitOnError)
	snapshotFlag := recoverFlags.String("snapshot", "", "Snapshot ID to recover")
	yesFlag := recoverFlags.Bool("yes", false, "Allow destructive recovery without yes/no prompt")
	confirmUserFlag := recoverFlags.String("confirm-user", "", "IMAP username confirmation for non-interactive destructive recovery")
	if err := recoverFlags.Parse(args); err != nil {
		return 1
	}

	config, err := internal.LoadConfig(configPath, envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}

	runtime, err := internal.StartRuntime(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	defer runtime.Close()

	app := &internal.RecoverApp{
		Config:          config,
		Runtime:         runtime,
		SnapshotFlag:    *snapshotFlag,
		YesFlag:         *yesFlag,
		ConfirmUserFlag: *confirmUserFlag,
	}
	if err := app.Run(); err != nil {
		internal.NotifyRuntimeFailure(runtime, err)
		return 1
	}
	return 0
}

func runRecoverResume(configPath, envPath string, args []string) int {
	resumeFlags := flag.NewFlagSet("recover-resume", flag.ExitOnError)
	runIDFlag := resumeFlags.String("run", "", "Recovery run ID to resume")
	if err := resumeFlags.Parse(args); err != nil {
		return 1
	}

	config, err := internal.LoadConfig(configPath, envPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}

	runtime, err := internal.StartRuntime(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}
	defer runtime.Close()

	app := &internal.RecoverResumeApp{
		Config:    config,
		Runtime:   runtime,
		RunIDFlag: *runIDFlag,
	}
	if err := app.Run(); err != nil {
		internal.NotifyRuntimeFailure(runtime, err)
		return 1
	}
	return 0
}
