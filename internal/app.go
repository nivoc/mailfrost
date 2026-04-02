package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiRed    = "\033[31m"
	ansiYellow = "\033[33m"
	ansiGreen  = "\033[32m"
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

	a.Runtime.Console(fmt.Sprintf(
		"%s: syncing %s@%s into %s",
		defaultToolName,
		strings.TrimSpace(a.Config.Env["IMAP_USERNAME"]),
		strings.TrimSpace(a.Config.Env["IMAP_HOST"]),
		a.Config.MaildirPath,
	))
	a.Runtime.Console(defaultToolName + ": sync start")
	if _, err := a.Runtime.RunCommand(a.Config.MbsyncCommand, nil); err != nil {
		return 0, err
	}
	a.Runtime.Console(defaultToolName + ": sync done")

	a.Runtime.Console(defaultToolName + ": audit start (scanning and hashing Maildir)")
	currentManifest, err := BuildManifest(a.Config.MaildirPath, a.Config.IgnoreMailboxRegex, time.Now())
	if err != nil {
		return 0, err
	}
	if err := WriteJSON(currentManifestPath, currentManifest); err != nil {
		return 0, fmt.Errorf("write current manifest: %w", err)
	}

	var report AuditReport
	if _, err := os.Stat(a.baselineManifestPath()); err == nil {
		a.Runtime.Console(defaultToolName + ": audit compare against baseline")
		a.Runtime.LogFile("INFO", "Comparing current Maildir state against baseline")
		baselineManifest, err := LoadManifest(a.baselineManifestPath())
		if err != nil {
			return 0, err
		}
		report = CompareManifests(baselineManifest, currentManifest, a.Config.ImmutabilityDays, a.Config.ReportSampleLimit, time.Now())
	} else {
		a.Runtime.Console(defaultToolName + ": audit baseline init")
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
	a.Runtime.Console(defaultToolName + ": audit done")
	a.printAuditSummary(report)

	a.Runtime.Console(defaultToolName + ": kopia backup start")
	snapshotID, snapshotSize, err := a.runKopiaSnapshotPath(a.Config.MaildirPath, a.Config.KopiaSnapshotArgs)
	if err != nil {
		return 0, err
	}
	if snapshotID != "" {
		a.Runtime.Console(fmt.Sprintf("%s: kopia backup done (snapshot %s, %s)", defaultToolName, snapshotID, snapshotSize))
	} else {
		a.Runtime.Console(defaultToolName + ": kopia backup done")
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
	a.printKopiaRepoStatus()

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
	a.Runtime.ConsoleRaw("\n")
	a.Runtime.ConsoleRaw(renderAuditSummaryBox(report))

	if strings.EqualFold(report.Summary.Status, "alert") {
		topFinding := firstTopFinding(report)
		if topFinding != "" {
			a.Runtime.ConsoleRaw("\nTOP FINDING\n")
			a.Runtime.ConsoleRaw(topFinding)
		}
	}
	a.Runtime.ConsoleRaw("\n")
}

func renderAuditSummaryBox(report AuditReport) string {
	const (
		labelWidth = 10
		valueWidth = 25
	)
	newMessages := report.Summary.CurrentUniqueMessages - report.Summary.BaselineUniqueMessages
	if newMessages < 0 {
		newMessages = 0
	}

	statusPlain := auditStatusLabel(report.Summary.Status)
	statusDisplay := auditColorizeStatus(statusPlain, report.Summary.Status)
	missingPlain := fmt.Sprintf("%d", report.Summary.MissingStableCount)
	mutatedPlain := fmt.Sprintf("%d", report.Summary.MutatedStableCount)
	movedPlain := fmt.Sprintf("%d", report.Summary.PlacementChangedStableCount)

	lines := []string{
		"+---------------- AUDIT ----------------+",
		auditBoxRow(labelWidth, valueWidth, "Status", statusPlain, statusDisplay),
		auditBoxRow(labelWidth, valueWidth, "Window", fmt.Sprintf("older than %d days", report.Summary.ImmutabilityDays), ""),
		auditBoxRow(labelWidth, valueWidth, "Indexed", fmt.Sprintf("%s mails", formatCount(report.Summary.CurrentUniqueMessages)), ""),
		auditBoxRow(labelWidth, valueWidth, "New", formatCount(newMessages), ""),
		auditBoxRow(labelWidth, valueWidth, "Missing", missingPlain, auditColorizeCount(missingPlain, report.Summary.MissingStableCount)),
		auditBoxRow(labelWidth, valueWidth, "Mutated", mutatedPlain, auditColorizeCount(mutatedPlain, report.Summary.MutatedStableCount)),
		auditBoxRow(labelWidth, valueWidth, "Moved/Lost", movedPlain, auditColorizeCount(movedPlain, report.Summary.PlacementChangedStableCount)),
		"+---------------------------------------+",
	}
	return strings.Join(lines, "\n") + "\n"
}

func auditBoxRow(labelWidth, valueWidth int, label, plainValue, styledValue string) string {
	paddedValue := fmt.Sprintf("%-*s", valueWidth, plainValue)
	if styledValue != "" && styledValue != plainValue {
		paddedValue = strings.Replace(paddedValue, plainValue, styledValue, 1)
	}
	return fmt.Sprintf("| %-*s %s |", labelWidth, label, paddedValue)
}

func auditStatusLabel(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "baseline-init":
		return "BASELINE INIT"
	case "alert":
		return "ALERT"
	default:
		return "OK"
	}
}

func auditColorizeStatus(value, status string) string {
	if !supportsANSIColor() {
		return value
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "baseline-init":
		return ansiBold + ansiYellow + value + ansiReset
	case "alert":
		return ansiBold + ansiRed + value + ansiReset
	default:
		return ansiBold + ansiGreen + value + ansiReset
	}
}

func auditColorizeCount(value string, count int) string {
	if count == 0 || !supportsANSIColor() {
		return value
	}
	return ansiBold + ansiRed + value + ansiReset
}

func supportsANSIColor() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	if (info.Mode() & os.ModeCharDevice) == 0 {
		return false
	}
	term := strings.TrimSpace(os.Getenv("TERM"))
	return term != "" && term != "dumb"
}

func formatCount(value int) string {
	text := fmt.Sprintf("%d", value)
	if len(text) <= 3 {
		return text
	}
	var parts []string
	for len(text) > 3 {
		parts = append([]string{text[len(text)-3:]}, parts...)
		text = text[:len(text)-3]
	}
	parts = append([]string{text}, parts...)
	return strings.Join(parts, ",")
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
		"--no-progress",
	}
}

func (a *App) kopiaSnapshotBaseArgs() []string {
	return []string{
		"--config-file", a.Config.KopiaConfigPath,
	}
}

func (a *App) kopiaCommandEnv() map[string]string {
	return map[string]string{
		"KOPIA_PASSWORD": a.Config.KopiaPassword,
	}
}

func (a *App) buildKopiaSnapshotCreateCommand(path string, extraArgs []string) []string {
	command := append([]string{}, a.Config.KopiaCommand...)
	command = append(command, "snapshot", "create")
	command = append(command, a.kopiaSnapshotBaseArgs()...)
	command = append(command, "--json")
	command = append(command, extraArgs...)
	if path == a.Config.MaildirPath {
		command = append(command, "--tags", "account:"+normalizedAccountTag(strings.TrimSpace(a.Config.Env["IMAP_USERNAME"])))
	}
	command = append(command, path)
	return command
}

func (a *App) runKopiaSnapshotPath(path string, extraArgs []string) (string, string, error) {
	command := a.buildKopiaSnapshotCreateCommand(path, extraArgs)
	output, err := a.Runtime.RunCommand(command, a.kopiaCommandEnv())
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
	if _, err := a.Runtime.RunCommand(command, a.kopiaCommandEnv()); err != nil {
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

func (a *App) printKopiaRepoStatus() {
	compression := a.kopiaCompressionSummary()
	status := buildKopiaRepoStatusFromConfig(a.Config, a.Runtime.Paths, compression)
	a.Runtime.ConsoleRaw(renderKopiaRepoStatus(status))
}

func (a *App) kopiaCompressionSummary() string {
	command := append([]string{}, a.Config.KopiaCommand...)
	command = append(command, "policy", "show")
	command = append(command, a.kopiaBaseArgs()...)
	command = append(command, a.Config.MaildirPath)
	output, err := a.Runtime.RunCommand(command, a.kopiaCommandEnv())
	if err != nil {
		a.Runtime.LogFile("WARN", fmt.Sprintf("Kopia policy show failed while building repo summary: %s", err))
		return "unknown"
	}
	return parseKopiaCompressionPolicyShow(output)
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
