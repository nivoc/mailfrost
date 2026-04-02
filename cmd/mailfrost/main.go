package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mailfrost/internal"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var version = "dev"

const (
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorCyan   = "\033[36m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
)

type releaseInfo struct {
	TagName string `json:"tag_name"`
	HTMLURL string `json:"html_url"`
}

type releaseCheckResult struct {
	info            releaseInfo
	checked         bool
	updateAvailable bool
}

func main() {
	os.Exit(runMain())
}

func runMain() int {
	defaultConfigPath := "config"
	defaultEnvPath := ".env"
	configPath := flag.String("config", defaultConfigPath, "Path to the non-secret config file")
	envPath := flag.String("env", defaultEnvPath, "Path to the .env secrets file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%sUsage:%s mailfrost [flags] [command]\n\n", colorHeader(), colorReset)
		fmt.Fprintf(os.Stderr, "%sCommands:%s\n", colorHeader(), colorReset)
		fmt.Fprintf(os.Stderr, "  %sbackup%s          Sync mail, audit stable messages, and create a kopia backup\n", colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, "  %srecover%s         Restore a snapshot and rewrite the managed IMAP mailboxes to match it\n", colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, "  %srecover-resume%s  Retry the last recovery mbsync push without clearing mailboxes again\n", colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, "  %srebaseline%s      Accept the current Maildir state as the new known-good baseline\n", colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, "  %srestore%s         Restore a Maildir snapshot from kopia\n", colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, "  %ssetup%s           Interactive setup wizard for mbsync and kopia\n", colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, "  %sversion%s         Show the Mailfrost version\n\n", colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, "%sFlags:%s\n", colorHeader(), colorReset)
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n%sRestore flags%s (use after %srestore%s):\n", colorHeader(), colorReset, colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, `  -snapshot string
        Snapshot ID to restore (skip interactive selection)
  -target string
        Restore target directory (default: ./restored/<snapshot-id>)
  -force
        Allow restoring directly into the configured MAILDIR_PATH without interactive confirmation
`)
		fmt.Fprintf(os.Stderr, "\n%sRecover flags%s (use after %srecover%s):\n", colorHeader(), colorReset, colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, `  -snapshot string
        Snapshot ID to recover (skip interactive selection)
  -yes
        Allow destructive recovery without yes/no prompt
  -confirm-user string
        IMAP username confirmation for non-interactive destructive recovery
`)
		fmt.Fprintf(os.Stderr, "\n%sRecover-resume flags%s (use after %srecover-resume%s):\n", colorHeader(), colorReset, colorCommand(), colorReset)
		fmt.Fprintf(os.Stderr, `  -run string
        Recovery run ID to resume (default: latest recovery mbsync config in state dir)
`)
	}
	flag.Parse()

	subcommand := ""
	if args := flag.Args(); len(args) > 0 {
		subcommand = args[0]
	}

	printVersionBanner()

	switch subcommand {
	case "":
		flag.Usage()
		return 0
	case "version":
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
		fmt.Fprintf(os.Stderr, "%sUnknown command:%s %s\n", colorWarning(), colorReset, subcommand)
		fmt.Fprintf(os.Stderr, "Usage: mailfrost [backup|recover|recover-resume|rebaseline|restore|setup|version]\n")
		return 1
	}
}

func versionString() string {
	return "mailfrost " + version
}

func printVersionBanner() {
	result := checkLatestRelease()
	lineColor := colorVersion()
	if result.updateAvailable {
		fmt.Println(lineColor + versionString() + colorReset)
		fmt.Printf("%sUpdate available:%s %s (current: %s)\n", colorWarning(), colorReset, result.info.TagName, version)
		fmt.Printf("%sUpgrade:%s brew update && brew upgrade mailfrost\n", colorWarning(), colorReset)
		if result.info.HTMLURL != "" {
			fmt.Printf("%sRelease:%s %s\n", colorWarning(), colorReset, result.info.HTMLURL)
		}
		return
	}
	if result.checked {
		lineColor = colorSuccess()
		fmt.Println(lineColor + versionString() + " [latest]" + colorReset)
		return
	}
	fmt.Println(lineColor + versionString() + colorReset)
}

func checkLatestRelease() releaseCheckResult {
	if strings.TrimSpace(version) == "" || version == "dev" {
		return releaseCheckResult{}
	}

	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/repos/nivoc/mailfrost/releases/latest", nil)
	if err != nil {
		return releaseCheckResult{}
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "mailfrost/"+version)

	client := &http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		return releaseCheckResult{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return releaseCheckResult{}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return releaseCheckResult{}
	}

	var info releaseInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return releaseCheckResult{}
	}
	if info.TagName == "" {
		return releaseCheckResult{}
	}
	return releaseCheckResult{
		info:            info,
		checked:         true,
		updateAvailable: isVersionNewer(info.TagName, version),
	}
}

func isVersionNewer(latest, current string) bool {
	latestParts, okLatest := parseVersionParts(latest)
	currentParts, okCurrent := parseVersionParts(current)
	if !okLatest || !okCurrent {
		return false
	}
	for i := 0; i < len(latestParts) || i < len(currentParts); i++ {
		latestPart := versionPart(latestParts, i)
		currentPart := versionPart(currentParts, i)
		if latestPart > currentPart {
			return true
		}
		if latestPart < currentPart {
			return false
		}
	}
	return false
}

func parseVersionParts(raw string) ([]int, bool) {
	cleaned := strings.TrimSpace(strings.TrimPrefix(raw, "v"))
	if cleaned == "" {
		return nil, false
	}
	chunks := strings.Split(cleaned, ".")
	parts := make([]int, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk == "" {
			return nil, false
		}
		end := 0
		for end < len(chunk) && chunk[end] >= '0' && chunk[end] <= '9' {
			end++
		}
		if end == 0 {
			return nil, false
		}
		value, err := strconv.Atoi(chunk[:end])
		if err != nil {
			return nil, false
		}
		parts = append(parts, value)
	}
	return parts, true
}

func versionPart(parts []int, idx int) int {
	if idx >= len(parts) {
		return 0
	}
	return parts[idx]
}

func colorHeader() string {
	return colorBold + colorCyan
}

func colorCommand() string {
	return colorBold
}

func colorWarning() string {
	return colorBold + colorRed
}

func colorVersion() string {
	return colorBold + colorYellow
}

func colorSuccess() string {
	return colorBold + colorGreen
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
