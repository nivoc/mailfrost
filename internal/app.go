package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type App struct {
	Config  Config
	Runtime *Runtime
}

func (a *App) RunBackup() (int, error) {
	if err := os.MkdirAll(a.Config.MaildirPath, 0o755); err != nil {
		return 0, fmt.Errorf("create maildir_path: %w", err)
	}
	if err := a.ensureGeneratedMbsyncConfig(); err != nil {
		return 0, err
	}

	currentManifestPath := a.currentManifestPath()
	reportJSONPath := a.reportJSONPath()
	reportTextPath := a.reportTextPath()

	a.Runtime.Console("mail-backup: sync start")
	if _, err := a.Runtime.RunCommand(a.Config.MbsyncCommand, nil); err != nil {
		return 0, err
	}
	a.Runtime.Console("mail-backup: sync done")

	currentManifest, err := BuildManifest(a.Config.MaildirPath, a.Config.IgnoreMailboxRegex, time.Now())
	if err != nil {
		return 0, err
	}
	if err := WriteJSON(currentManifestPath, currentManifest); err != nil {
		return 0, fmt.Errorf("write current manifest: %w", err)
	}

	var report AuditReport
	if _, err := os.Stat(a.baselineManifestPath()); err == nil {
		a.Runtime.LogFile("INFO", "Comparing current Maildir state against baseline")
		baselineManifest, err := LoadManifest(a.baselineManifestPath())
		if err != nil {
			return 0, err
		}
		report = CompareManifests(baselineManifest, currentManifest, a.Config.ImmutabilityDays, a.Config.ReportSampleLimit, time.Now())
	} else {
		a.Runtime.LogFile("INFO", "No baseline found, this run will initialize it after a successful backup")
		report = BaselineInitReport(a.Config.ImmutabilityDays, currentManifest.Stats.UniqueMessages)
	}

	reportText := RenderReportText(report, a.Config.ReportSampleLimit)
	if err := WriteJSON(reportJSONPath, report); err != nil {
		return 0, fmt.Errorf("write report json: %w", err)
	}
	if err := WriteText(reportTextPath, reportText); err != nil {
		return 0, fmt.Errorf("write report text: %w", err)
	}
	a.printAuditSummary(report)

	a.Runtime.Console("mail-backup: kopia backup start")
	snapshotID, snapshotSize, err := a.runKopiaSnapshotPath(a.Config.MaildirPath, a.Config.KopiaSnapshotArgs)
	if err != nil {
		return 0, err
	}
	if snapshotID != "" {
		a.Runtime.Console(fmt.Sprintf("mail-backup: kopia backup done (snapshot %s, %s)", snapshotID, snapshotSize))
	} else {
		a.Runtime.Console("mail-backup: kopia backup done")
	}

	if a.Config.KopiaIncludeStateDir {
		if _, _, err := a.runKopiaSnapshotPath(a.Config.StateDir, nil); err != nil {
			a.Runtime.LogFile("WARN", fmt.Sprintf("State dir snapshot failed: %s", err))
		}
	} else {
		for _, path := range []string{currentManifestPath, reportJSONPath, reportTextPath} {
			if _, _, err := a.runKopiaSnapshotPath(path, nil); err != nil {
				a.Runtime.LogFile("WARN", fmt.Sprintf("Auxiliary snapshot failed for %s: %s", path, err))
			}
		}
	}

	if err := a.maybeRunKopiaMaintenance(); err != nil {
		return 0, err
	}

	switch report.Summary.Status {
	case "alert":
		a.handleIntegrityAlert(report, reportTextPath, currentManifestPath)
		a.printFinalStatus("ALERT", "baseline: not updated", AlertExitCode)
		return AlertExitCode, nil
	case "baseline-init":
		a.Runtime.LogFile("INFO", "Initializing baseline after first successful backup")
	default:
		a.Runtime.LogFile("INFO", "Audit completed without stable-message anomalies")
	}

	if err := a.promoteBaseline(currentManifestPath, true); err != nil {
		return 0, err
	}
	a.Runtime.LogFile("INFO", fmt.Sprintf("Audit report: %s", reportJSONPath))
	a.Runtime.LogFile("INFO", fmt.Sprintf("Audit summary: %s", reportTextPath))
	a.printFinalStatus("OK", "", 0)
	return 0, nil
}

func (a *App) RunRebaseline() error {
	if err := os.MkdirAll(a.Config.MaildirPath, 0o755); err != nil {
		return fmt.Errorf("create maildir_path: %w", err)
	}

	currentManifestPath := a.currentManifestPath()
	manifest, err := BuildManifest(a.Config.MaildirPath, a.Config.IgnoreMailboxRegex, time.Now())
	if err != nil {
		return err
	}
	if err := WriteJSON(currentManifestPath, manifest); err != nil {
		return fmt.Errorf("write current manifest: %w", err)
	}

	a.Runtime.Console("Backing up current Maildir state before rebaseline...")
	if _, _, err := a.runKopiaSnapshotPath(a.Config.MaildirPath, a.Config.KopiaSnapshotArgs); err != nil {
		return err
	}
	if a.Config.KopiaIncludeStateDir {
		if _, _, err := a.runKopiaSnapshotPath(a.Config.StateDir, nil); err != nil {
			a.Runtime.LogFile("WARN", fmt.Sprintf("State dir snapshot failed: %s", err))
		}
	} else {
		if _, _, err := a.runKopiaSnapshotPath(currentManifestPath, nil); err != nil {
			a.Runtime.LogFile("WARN", fmt.Sprintf("Manifest snapshot failed: %s", err))
		}
	}

	if err := a.maybeRunKopiaMaintenance(); err != nil {
		return err
	}
	if err := a.promoteBaseline(currentManifestPath, false); err != nil {
		return err
	}
	a.Runtime.LogFile("INFO", "Current Maildir state accepted as the new baseline after a successful backup")
	return nil
}

func (a *App) baselineManifestPath() string {
	return filepath.Join(a.Runtime.Paths.ManifestDir, "baseline.json")
}

func (a *App) latestManifestPath() string {
	return filepath.Join(a.Runtime.Paths.ManifestDir, "latest.json")
}

func (a *App) currentManifestPath() string {
	return filepath.Join(a.Runtime.Paths.ManifestDir, fmt.Sprintf("manifest-%s.json", a.Runtime.RunID))
}

func (a *App) reportJSONPath() string {
	return filepath.Join(a.Runtime.Paths.ReportDir, fmt.Sprintf("report-%s.json", a.Runtime.RunID))
}

func (a *App) reportTextPath() string {
	return filepath.Join(a.Runtime.Paths.ReportDir, fmt.Sprintf("report-%s.txt", a.Runtime.RunID))
}

func (a *App) printAuditSummary(report AuditReport) {
	a.Runtime.ConsoleRaw("\nAUDIT\n")
	a.Runtime.ConsoleRaw(fmt.Sprintf("status: %s\n", strings.ToUpper(report.Summary.Status)))
	a.Runtime.ConsoleRaw(fmt.Sprintf("scope: monitoring messages older than %d days\n", report.Summary.ImmutabilityDays))
	a.Runtime.ConsoleRaw(
		fmt.Sprintf(
			"messages: baseline=%d current=%d\n",
			report.Summary.BaselineUniqueMessages,
			report.Summary.CurrentUniqueMessages,
		),
	)
	a.Runtime.ConsoleRaw(
		fmt.Sprintf(
			"findings: missing=%d mutated=%d moved_or_copy_loss=%d\n",
			report.Summary.MissingStableCount,
			report.Summary.MutatedStableCount,
			report.Summary.PlacementChangedStableCount,
		),
	)

	if strings.EqualFold(report.Summary.Status, "alert") {
		topFinding := firstTopFinding(report)
		if topFinding != "" {
			a.Runtime.ConsoleRaw("\nTOP FINDING\n")
			a.Runtime.ConsoleRaw(topFinding)
		}
	}
	a.Runtime.ConsoleRaw("\n")
}

func (a *App) printFinalStatus(status, detail string, exitCode int) {
	a.Runtime.ConsoleRaw("\nFINAL\n")
	a.Runtime.ConsoleRaw(fmt.Sprintf("status: %s\n", status))
	if strings.TrimSpace(detail) != "" {
		a.Runtime.ConsoleRaw(detail + "\n")
	}
	a.Runtime.ConsoleRaw(fmt.Sprintf("exit code: %d\n", exitCode))
}

func (a *App) ensureGeneratedMbsyncConfig() error {
	host := strings.TrimSpace(a.Config.Env["IMAP_HOST"])
	port := strings.TrimSpace(a.Config.Env["IMAP_PORT"])
	username := strings.TrimSpace(a.Config.Env["IMAP_USERNAME"])
	password := strings.TrimSpace(a.Config.Env["IMAP_PASSWORD"])
	if host == "" {
		return fmt.Errorf("IMAP_HOST must be set in .env")
	}
	if username == "" {
		return fmt.Errorf("IMAP_USERNAME must be set in .env")
	}
	if password == "" {
		return fmt.Errorf("IMAP_PASSWORD must be set in .env")
	}
	if port == "" {
		port = "993"
	}
	if err := writeGeneratedMbsyncConfig(a.Config.MbsyncConfigPath, host, port, username, a.Config.MaildirPath); err != nil {
		return err
	}
	a.Runtime.LogFile("INFO", fmt.Sprintf("Generated mbsync config: %s", a.Config.MbsyncConfigPath))
	return nil
}

func (a *App) kopiaBaseArgs() []string {
	return []string{
		"--config-file", a.Config.KopiaConfigPath,
		"--password", a.Config.KopiaPassword,
		"--no-progress",
	}
}

func (a *App) runKopiaSnapshotPath(path string, extraArgs []string) (string, string, error) {
	command := append([]string{}, a.Config.KopiaCommand...)
	command = append(command, "snapshot", "create")
	command = append(command, a.kopiaBaseArgs()...)
	command = append(command, "--json")
	command = append(command, extraArgs...)
	command = append(command, path)

	output, err := a.Runtime.RunCommand(command, nil)
	if err != nil {
		return "", "", err
	}
	return parseKopiaSnapshotOutput(output), parseKopiaSnapshotSize(output), nil
}

func (a *App) maybeRunKopiaMaintenance() error {
	if a.Config.KopiaMaintenanceIntervalDays == 0 {
		a.Runtime.LogFile("INFO", "Skipping kopia maintenance because kopia_maintenance_interval_days=0")
		return nil
	}
	if !isFileDueByDays(a.Runtime.Paths.KopiaMaintenanceStamp, a.Config.KopiaMaintenanceIntervalDays) {
		a.Runtime.LogFile("INFO", "Skipping kopia maintenance because it is not due yet")
		return nil
	}
	command := append([]string{}, a.Config.KopiaCommand...)
	command = append(command, "maintenance", "run", "--full")
	command = append(command, a.kopiaBaseArgs()...)
	if _, err := a.Runtime.RunCommand(command, nil); err != nil {
		return err
	}
	file, err := os.OpenFile(a.Runtime.Paths.KopiaMaintenanceStamp, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	now := time.Now()
	return os.Chtimes(a.Runtime.Paths.KopiaMaintenanceStamp, now, now)
}

func (a *App) promoteBaseline(sourceManifest string, preserveHistory bool) error {
	promotedManifest, err := LoadManifest(sourceManifest)
	if err != nil {
		return err
	}
	if preserveHistory {
		if _, err := os.Stat(a.baselineManifestPath()); err == nil {
			previousManifest, err := LoadManifest(a.baselineManifestPath())
			if err != nil {
				return err
			}
			promotedManifest = MergeManifestHistory(previousManifest, promotedManifest)
		}
	}
	if err := WriteManifestFile(a.latestManifestPath(), promotedManifest); err != nil {
		return err
	}
	if err := WriteManifestFile(a.baselineManifestPath(), promotedManifest); err != nil {
		return err
	}
	a.Runtime.LogFile("INFO", fmt.Sprintf("Baseline updated: %s", a.baselineManifestPath()))
	return nil
}

func (a *App) storeLatestOnly(sourceManifest string) error {
	latestManifest, err := LoadManifest(sourceManifest)
	if err != nil {
		return err
	}
	if err := WriteManifestFile(a.latestManifestPath(), latestManifest); err != nil {
		return err
	}
	a.Runtime.LogFile("INFO", "Latest manifest updated without promoting baseline")
	return nil
}

func (a *App) handleIntegrityAlert(report AuditReport, reportTextPath, currentManifestPath string) {
	a.Runtime.LogFile("ERROR", "Stable-message anomalies detected")
	a.Runtime.LogFile("ERROR", fmt.Sprintf("Missing stable messages: %d", report.Summary.MissingStableCount))
	a.Runtime.LogFile("ERROR", fmt.Sprintf("Mutated stable messages: %d", report.Summary.MutatedStableCount))
	a.Runtime.LogFile("ERROR", fmt.Sprintf("Stable messages with mailbox/count changes: %d", report.Summary.PlacementChangedStableCount))
	body, _ := os.ReadFile(reportTextPath)
	a.Runtime.SendAlert("ALERT", "Mail backup integrity alert", string(body))
	if err := a.storeLatestOnly(currentManifestPath); err != nil {
		a.Runtime.LogFile("WARN", fmt.Sprintf("Update latest manifest after alert failed: %s", err))
	}
	a.Runtime.LogFile("INFO", fmt.Sprintf("Audit report: %s", a.reportJSONPath()))
	a.Runtime.LogFile("INFO", fmt.Sprintf("Audit summary: %s", reportTextPath))
}

type kopiaSnapshotResult struct {
	ID        string `json:"id"`
	RootEntry *struct {
		Summary *struct {
			Size int64 `json:"size"`
		} `json:"summ"`
	} `json:"rootEntry"`
}

func parseKopiaSnapshotOutput(output string) string {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var result kopiaSnapshotResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			continue
		}
		if result.ID == "" {
			continue
		}
		if len(result.ID) > 10 {
			return result.ID[:10]
		}
		return result.ID
	}
	return ""
}

func parseKopiaSnapshotSize(output string) string {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var result kopiaSnapshotResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			continue
		}
		if result.RootEntry == nil || result.RootEntry.Summary == nil {
			return ""
		}
		return formatBytes(result.RootEntry.Summary.Size)
	}
	return ""
}

func formatBytes(bytes int64) string {
	switch {
	case bytes >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(bytes)/float64(1<<30))
	case bytes >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(bytes)/float64(1<<20))
	case bytes >= 1<<10:
		return fmt.Sprintf("%.3f KiB", float64(bytes)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func isFileDueByDays(path string, intervalDays int) bool {
	info, err := os.Stat(path)
	if err != nil {
		return true
	}
	age := time.Since(info.ModTime())
	return age >= time.Duration(intervalDays)*24*time.Hour
}

func firstTopFinding(report AuditReport) string {
	if len(report.Samples.MissingStableMessages) > 0 {
		item := report.Samples.MissingStableMessages[0]
		return strings.Join([]string{
			fmt.Sprintf("missing: %s", reportMessageID(item.MessageID, item.Key)),
			fmt.Sprintf("subject: %s", reportSubject(item.Subject)),
			fmt.Sprintf("mailbox: %s", firstOrDash(item.Mailboxes)),
			fmt.Sprintf("date: %s", item.MessageDate),
		}, "\n") + "\n"
	}
	if len(report.Samples.MutatedStableMessages) > 0 {
		item := report.Samples.MutatedStableMessages[0]
		return strings.Join([]string{
			fmt.Sprintf("mutated: %s", reportMessageID(item.MessageID, item.Key)),
			fmt.Sprintf("subject: %s", reportSubject(item.Subject)),
			fmt.Sprintf("mailbox: %s", firstOrDash(item.Mailboxes)),
			fmt.Sprintf("date: %s", item.MessageDate),
		}, "\n") + "\n"
	}
	if len(report.Samples.PlacementChangedStableMessages) > 0 {
		item := report.Samples.PlacementChangedStableMessages[0]
		return strings.Join([]string{
			fmt.Sprintf("moved_or_copy_loss: %s", reportMessageID(item.MessageID, item.Key)),
			fmt.Sprintf("subject: %s", reportSubject(item.Subject)),
			fmt.Sprintf("mailbox_before: %s", firstOrDash(item.BaselineMailboxes)),
			fmt.Sprintf("mailbox_now: %s", firstOrDash(item.CurrentMailboxes)),
			fmt.Sprintf("date: %s", item.MessageDate),
		}, "\n") + "\n"
	}
	return ""
}

func firstOrDash(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return values[0]
}
