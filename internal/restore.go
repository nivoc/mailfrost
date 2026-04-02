package internal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type RestoreApp struct {
	Config       Config
	Runtime      *Runtime
	SnapshotFlag string
	TargetFlag   string
	ForceFlag    bool
	stdin        *bufio.Reader
}

type snapshotScope int

const (
	snapshotScopeAccount snapshotScope = iota
	snapshotScopeAllMailBackup
)

type kopiaSnapshot struct {
	ID        string          `json:"id"`
	Source    kopiaSource     `json:"source"`
	StartTime string          `json:"startTime"`
	EndTime   string          `json:"endTime"`
	RootEntry *kopiaRootEntry `json:"rootEntry,omitempty"`
}

type kopiaSource struct {
	Path string `json:"path"`
}

type kopiaRootEntry struct {
	Summary *kopiaSummary `json:"summ,omitempty"`
}

type kopiaSummary struct {
	Size  int64 `json:"size"`
	Files int   `json:"files"`
}

func (s kopiaSnapshot) ShortID() string {
	if len(s.ID) > 10 {
		return s.ID[:10]
	}
	return s.ID
}

func (s kopiaSnapshot) FormatTime() string {
	t, err := time.Parse(time.RFC3339Nano, s.StartTime)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s.StartTime)
	}
	if err != nil {
		return s.StartTime
	}
	return t.Local().Format("2006-01-02 15:04")
}

func (a *RestoreApp) Run() error {
	a.stdin = bufio.NewReader(os.Stdin)

	a.Runtime.Console("Loading snapshots...")
	snapshots, scope, err := a.loadSnapshotsForSelection(10)
	if err != nil {
		return err
	}
	if len(snapshots) == 0 {
		a.Runtime.Console("No snapshots found")
		return nil
	}

	var selected kopiaSnapshot
	if a.SnapshotFlag != "" {
		selected, err = a.findSnapshot(a.SnapshotFlag)
		if err != nil {
			return err
		}
	} else {
		selected, err = a.promptSnapshot(snapshots, scope)
		if err != nil {
			return err
		}
	}

	target, err := a.resolveRestoreTarget(selected)
	if err != nil {
		return err
	}
	if err := a.restoreSnapshot(selected, target); err != nil {
		return err
	}
	a.Runtime.Console(fmt.Sprintf("Restored snapshot %s to %s", selected.ShortID(), target))
	return nil
}

func (a *RestoreApp) kopiaBaseArgs() []string {
	return []string{
		"--config-file", a.Config.KopiaConfigPath,
		"--password", a.Config.KopiaPassword,
		"--no-progress",
	}
}

func (a *RestoreApp) listSnapshots(limit int) ([]kopiaSnapshot, error) {
	return a.listSnapshotsForScope(limit, snapshotScopeAccount)
}

func (a *RestoreApp) listSnapshotsForScope(limit int, scope snapshotScope) ([]kopiaSnapshot, error) {
	seen := map[string]kopiaSnapshot{}
	for _, purposeTag := range []string{currentKopiaPurposeTag, legacyKopiaPurposeTag} {
		command := append([]string{}, a.Config.KopiaCommand...)
		command = append(command, "snapshot", "list", "--json")
		command = append(command, a.kopiaBaseArgs()...)
		command = append(command, "--tags", purposeTag)
		if scope == snapshotScopeAccount {
			command = append(command, "--tags", "account:"+normalizedAccountTag(strings.TrimSpace(a.Config.Env["IMAP_USERNAME"])))
		}
		command = append(command, "-n", strconv.Itoa(limit))

		output, err := a.Runtime.RunCommand(command, nil)
		if err != nil {
			return nil, fmt.Errorf("list snapshots: %w", err)
		}

		var snapshots []kopiaSnapshot
		if err := json.Unmarshal([]byte(output), &snapshots); err != nil {
			return nil, fmt.Errorf("parse snapshot list: %w", err)
		}
		for _, snapshot := range snapshots {
			seen[snapshot.ID] = snapshot
		}
	}
	snapshots := make([]kopiaSnapshot, 0, len(seen))
	for _, snapshot := range seen {
		snapshots = append(snapshots, snapshot)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].StartTime > snapshots[j].StartTime
	})
	return snapshots, nil
}

func (a *RestoreApp) findSnapshot(prefix string) (kopiaSnapshot, error) {
	snapshots, err := a.listSnapshotsForScope(100, snapshotScopeAccount)
	if err != nil {
		return kopiaSnapshot{}, err
	}
	for _, snapshot := range snapshots {
		if strings.HasPrefix(snapshot.ID, prefix) {
			return snapshot, nil
		}
	}
	allSnapshots, err := a.listSnapshotsForScope(100, snapshotScopeAllMailBackup)
	if err != nil {
		return kopiaSnapshot{}, err
	}
	if len(allSnapshots) > 0 {
		ok, err := a.promptYesNo("No snapshots found for the configured account. Show snapshots for all Mailfrost accounts in this repository", false)
		if err != nil {
			return kopiaSnapshot{}, err
		}
		if ok {
			for _, snapshot := range allSnapshots {
				if strings.HasPrefix(snapshot.ID, prefix) {
					return snapshot, nil
				}
			}
		}
	}
	return kopiaSnapshot{}, fmt.Errorf("no snapshot matching %q found", prefix)
}

func (a *RestoreApp) promptSnapshot(snapshots []kopiaSnapshot, scope snapshotScope) (kopiaSnapshot, error) {
	const pad = "         "
	loaded := snapshots
	pageSize := len(snapshots)

	for {
		a.Runtime.ConsoleRaw(fmt.Sprintf("%sAvailable snapshots:\n", pad))
		if scope == snapshotScopeAllMailBackup {
			a.Runtime.ConsoleRaw(fmt.Sprintf("%s  showing all Mailfrost snapshots in this repository\n", pad))
		}
		for i, snapshot := range loaded {
			summary := ""
			if snapshot.RootEntry != nil && snapshot.RootEntry.Summary != nil {
				summary = fmt.Sprintf("  %s", formatBytes(snapshot.RootEntry.Summary.Size))
			}
			a.Runtime.ConsoleRaw(fmt.Sprintf("%s  #%-3d %s  %s%s\n", pad, i+1, snapshot.ShortID(), snapshot.FormatTime(), summary))
		}
		if len(loaded) >= pageSize {
			a.Runtime.ConsoleRaw(fmt.Sprintf("%s  [m]  Show more\n", pad))
		}
		a.Runtime.ConsoleRaw("\n")

		input, err := a.prompt(fmt.Sprintf("%sSelect snapshot [1]: ", pad))
		if err != nil {
			return kopiaSnapshot{}, err
		}
		if input == "" {
			return loaded[0], nil
		}
		if input == "m" {
			pageSize += 10
			more, err := a.listSnapshotsForScope(pageSize, scope)
			if err != nil {
				return kopiaSnapshot{}, err
			}
			loaded = more
			continue
		}
		num, err := strconv.Atoi(input)
		if err != nil || num < 1 || num > len(loaded) {
			a.Runtime.ConsoleRaw(fmt.Sprintf("%sInvalid selection, try again\n\n", pad))
			continue
		}
		return loaded[num-1], nil
	}
}

func (a *RestoreApp) loadSnapshotsForSelection(limit int) ([]kopiaSnapshot, snapshotScope, error) {
	snapshots, err := a.listSnapshotsForScope(limit, snapshotScopeAccount)
	if err != nil {
		return nil, snapshotScopeAccount, err
	}
	allSnapshots, err := a.listSnapshotsForScope(limit, snapshotScopeAllMailBackup)
	if err != nil {
		return nil, snapshotScopeAccount, err
	}
	return a.chooseSnapshotScope(snapshots, allSnapshots)
}

func (a *RestoreApp) chooseSnapshotScope(accountSnapshots, allSnapshots []kopiaSnapshot) ([]kopiaSnapshot, snapshotScope, error) {
	if len(accountSnapshots) > 0 {
		return accountSnapshots, snapshotScopeAccount, nil
	}
	if len(allSnapshots) == 0 {
		return nil, snapshotScopeAccount, nil
	}

	ok, err := a.promptYesNo("No snapshots found for the configured account. Show snapshots for all Mailfrost accounts in this repository", false)
	if err != nil {
		return nil, snapshotScopeAccount, err
	}
	if !ok {
		return nil, snapshotScopeAccount, nil
	}
	return allSnapshots, snapshotScopeAllMailBackup, nil
}

func (a *RestoreApp) resolveRestoreTarget(snapshot kopiaSnapshot) (string, error) {
	if strings.TrimSpace(a.TargetFlag) != "" {
		target, err := filepath.Abs(a.TargetFlag)
		if err != nil {
			return "", err
		}
		if err := a.validateRestoreTarget(target); err != nil {
			return "", err
		}
		return target, nil
	}

	defaultTarget := filepath.Join(filepath.Dir(a.Config.ConfigPath), "restored", snapshot.ShortID())
	if err := os.MkdirAll(filepath.Dir(defaultTarget), 0o755); err != nil {
		return "", err
	}
	if err := a.validateRestoreTarget(defaultTarget); err != nil {
		return "", err
	}
	return defaultTarget, nil
}

func (a *RestoreApp) validateRestoreTarget(target string) error {
	configuredMaildir, err := filepath.Abs(a.Config.MaildirPath)
	if err != nil {
		return err
	}
	if samePath(target, configuredMaildir) {
		if a.ForceFlag {
			return nil
		}
		ok, err := a.promptYesNo("Restore directly into the configured MAILDIR_PATH", false)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("restore aborted")
		}
		return nil
	}

	entries, err := os.ReadDir(target)
	if err == nil && len(entries) > 0 {
		return fmt.Errorf("restore target is not empty: %s", target)
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (a *RestoreApp) restoreSnapshot(snapshot kopiaSnapshot, target string) error {
	if err := os.MkdirAll(target, 0o755); err != nil {
		return fmt.Errorf("create restore target: %w", err)
	}
	a.Runtime.Console(fmt.Sprintf("Restoring snapshot %s (%s)...", snapshot.ShortID(), snapshot.FormatTime()))
	command := append([]string{}, a.Config.KopiaCommand...)
	command = append(command, "snapshot", "restore")
	command = append(command, a.kopiaBaseArgs()...)
	command = append(command, snapshot.ID, target)
	if _, err := a.Runtime.RunCommand(command, nil); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	return nil
}

func samePath(left, right string) bool {
	cleanLeft := filepath.Clean(left)
	cleanRight := filepath.Clean(right)
	return cleanLeft == cleanRight
}

func (a *RestoreApp) prompt(label string) (string, error) {
	if label != "" {
		fmt.Print(label)
	}
	line, err := a.stdin.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func (a *RestoreApp) promptYesNo(label string, defaultValue bool) (bool, error) {
	defaultText := "n"
	if defaultValue {
		defaultText = "y"
	}
	fmt.Printf("%s [%s]: ", label, defaultText)
	value, err := a.prompt("")
	if err != nil {
		return false, err
	}
	if value == "" {
		value = defaultText
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid selection: %s", value)
	}
}
