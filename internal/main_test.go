package internal

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaultsUseToolLocalSubdirs(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config")
	envPath := filepath.Join(tempDir, ".env")
	mustWriteFile(t, envPath, strings.Join([]string{
		"KOPIA_CONFIG_PATH=./data/kopia/repository.config",
		"KOPIA_PASSWORD=testpass",
		"KOPIA_REPO_TYPE=filesystem",
		"KOPIA_REPO_PATH=./data/kopia/repo",
		"IMAP_PASSWORD=secret",
	}, "\n")+"\n")
	mustWriteFile(t, configPath, strings.Join([]string{
		"MBSYNC_COMMAND=/bin/echo mbsync",
		"KOPIA_COMMAND=/bin/echo kopia",
	}, "\n")+"\n")

	config, err := LoadConfig(configPath, envPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.MaildirPath != filepath.Join(tempDir, "data", "maildir") {
		t.Fatalf("MaildirPath = %s", config.MaildirPath)
	}
	if config.StateDir != filepath.Join(tempDir, "data", "state") {
		t.Fatalf("StateDir = %s", config.StateDir)
	}
}

func TestLoadConfigSplitsOperationalConfigAndEnvSecrets(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config")
	envPath := filepath.Join(tempDir, ".env")
	mustWriteFile(t, configPath, strings.Join([]string{
		"STATE_DIR=./var/state",
		"REPORT_SAMPLE_LIMIT=5",
		"MBSYNC_COMMAND=/bin/echo mbsync",
		"KOPIA_COMMAND=/bin/echo kopia",
	}, "\n")+"\n")
	mustWriteFile(t, envPath, strings.Join([]string{
		"KOPIA_CONFIG_PATH=./data/kopia/repository.config",
		"KOPIA_PASSWORD=testpass",
		"KOPIA_REPO_TYPE=filesystem",
		"KOPIA_REPO_PATH=./data/kopia/repo",
		"IMAP_PASSWORD=pass",
	}, "\n")+"\n")

	config, err := LoadConfig(configPath, envPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if config.StateDir != filepath.Join(tempDir, "var", "state") {
		t.Fatalf("StateDir = %s", config.StateDir)
	}
	if config.ReportSampleLimit != 5 {
		t.Fatalf("ReportSampleLimit = %d", config.ReportSampleLimit)
	}
	if config.KopiaConfigPath != filepath.Join(tempDir, "data", "kopia", "repository.config") {
		t.Fatalf("KopiaConfigPath = %s", config.KopiaConfigPath)
	}
	if config.KopiaPassword != "testpass" {
		t.Fatalf("KopiaPassword = %s", config.KopiaPassword)
	}
}

func TestLoadConfigRejectsRuntimeSettingInsideDotEnv(t *testing.T) {
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config")
	envPath := filepath.Join(tempDir, ".env")
	mustWriteFile(t, configPath, "MBSYNC_COMMAND=/bin/echo mbsync\nKOPIA_COMMAND=/bin/echo kopia\n")
	mustWriteFile(t, envPath, strings.Join([]string{
		"KOPIA_CONFIG_PATH=./data/kopia/repository.config",
		"KOPIA_PASSWORD=testpass",
		"KOPIA_REPO_TYPE=filesystem",
		"KOPIA_REPO_PATH=./data/kopia/repo",
		"REPORT_SAMPLE_LIMIT=5",
	}, "\n")+"\n")

	_, err := LoadConfig(configPath, envPath)
	if err == nil || !strings.Contains(err.Error(), "REPORT_SAMPLE_LIMIT belongs in config") {
		t.Fatalf("LoadConfig() error = %v, want misplaced runtime setting", err)
	}
}

func TestBuildManifestRejectsMissingMaildir(t *testing.T) {
	_, err := BuildManifest(filepath.Join(t.TempDir(), "missing"), defaultIgnoreMailboxRegex, time.Unix(0, 0))
	if err == nil {
		t.Fatalf("BuildManifest() error = nil, want missing maildir error")
	}
}

func TestDefaultIgnoreRegexMatchesDotMaildirFolders(t *testing.T) {
	pattern := regexpMustCompile(t, defaultIgnoreMailboxRegex)
	if !pattern.MatchString(".Trash") {
		t.Fatalf("expected .Trash to match")
	}
	if !pattern.MatchString(".Spam.sub") {
		t.Fatalf("expected .Spam.sub to match")
	}
	if !pattern.MatchString("INBOX/Trash") {
		t.Fatalf("expected INBOX/Trash to match")
	}
	if pattern.MatchString("Archive") {
		t.Fatalf("did not expect Archive to match")
	}
}

func TestCompareAlertsOnMailboxOrCopyLoss(t *testing.T) {
	baseline := Manifest{
		Records: map[string]ManifestRecord{
			"mid:<a>": manifestRecord("mid:<a>", 0, 0, 2, []string{"INBOX", ".Archive"}, []string{"hash-a"}),
		},
	}
	current := Manifest{
		Records: map[string]ManifestRecord{
			"mid:<a>": manifestRecord("mid:<a>", 0, 0, 1, []string{"INBOX"}, []string{"hash-a"}),
		},
	}
	report := CompareManifests(baseline, current, 1, 20, time.Unix(86400*3, 0))
	if report.Summary.Status != "alert" {
		t.Fatalf("status = %s, want alert", report.Summary.Status)
	}
	if report.Summary.PlacementChangedStableCount != 1 {
		t.Fatalf("placement changes = %d", report.Summary.PlacementChangedStableCount)
	}
}

func TestCompareUsesFirstSeenForStability(t *testing.T) {
	baseline := Manifest{
		Records: map[string]ManifestRecord{
			"mid:<future>": manifestRecord("mid:<future>", 4102444800, 0, 1, []string{"INBOX"}, []string{"hash-a"}),
		},
	}
	current := Manifest{Records: map[string]ManifestRecord{}}
	report := CompareManifests(baseline, current, 1, 20, time.Unix(86400*3, 0))
	if report.Summary.Status != "alert" {
		t.Fatalf("status = %s, want alert", report.Summary.Status)
	}
	if report.Summary.MissingStableCount != 1 {
		t.Fatalf("missing = %d", report.Summary.MissingStableCount)
	}
}

func TestLoadManifestRejectsChecksumMismatch(t *testing.T) {
	manifest := Manifest{
		SchemaVersion:      currentManifestSchemaVersion,
		GeneratedAt:        "2026-01-01T00:00:00Z",
		Maildir:            "/tmp/maildir",
		IgnoreMailboxRegex: defaultIgnoreMailboxRegex,
		Stats: ManifestStats{
			FilesScanned:   1,
			UniqueMessages: 1,
		},
		Records: map[string]ManifestRecord{
			"mid:<a>": manifestRecord("mid:<a>", 0, 0, 1, []string{"INBOX"}, []string{"hash-a"}),
		},
	}
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "baseline.json")
	if err := WriteJSON(path, manifest); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}
	if err := writeChecksum(path); err != nil {
		t.Fatalf("writeChecksum() error = %v", err)
	}
	if err := os.WriteFile(path, []byte("{\"tampered\": true}\n"), 0o644); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}
	if _, err := LoadManifest(path); err == nil {
		t.Fatalf("LoadManifest() error = nil, want checksum mismatch")
	}
}

func TestSetupPromptOptionalAllowsClearingExistingValue(t *testing.T) {
	app := &SetupApp{stdin: bufio.NewReader(strings.NewReader("/\n"))}
	value, err := app.promptOptional("S3 prefix", "existing/", false)
	if err != nil {
		t.Fatalf("promptOptional() error = %v", err)
	}
	if value != "" {
		t.Fatalf("promptOptional() = %q, want empty string", value)
	}
}

func TestBuildKopiaCompressionPolicyCommandUsesZstd(t *testing.T) {
	app := &SetupApp{}
	got := app.buildKopiaCompressionPolicyCommand("/tmp/repository.config", "/tmp/maildir")
	want := []string{
		"kopia", "policy", "set",
		"--config-file", "/tmp/repository.config",
		"/tmp/maildir",
		"--compression", "zstd",
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("buildKopiaCompressionPolicyCommand() = %v, want %v", got, want)
	}
}

func TestBuildKopiaSnapshotCreateCommandUsesSinglePurposeTagAndAccountTag(t *testing.T) {
	app := &App{
		Config: Config{
			KopiaCommand:      []string{"kopia"},
			KopiaConfigPath:   "/tmp/repository.config",
			KopiaPassword:     "secret",
			MaildirPath:       "/tmp/maildir",
			KopiaSnapshotArgs: []string{"--tags", currentKopiaPurposeTag},
			Env: map[string]string{
				"IMAP_USERNAME": "Example+Inbox@example.com",
			},
		},
	}

	got := app.buildKopiaSnapshotCreateCommand(app.Config.MaildirPath, app.Config.KopiaSnapshotArgs)
	want := []string{
		"kopia", "snapshot", "create",
		"--config-file", "/tmp/repository.config",
		"--json",
		"--tags", currentKopiaPurposeTag,
		"--tags", "account:example-inbox-example-com",
		"/tmp/maildir",
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("buildKopiaSnapshotCreateCommand() = %v, want %v", got, want)
	}
}

func TestSanitizeCommandForDisplayRedactsPasswordFlags(t *testing.T) {
	got := sanitizeCommandForDisplay([]string{
		"kopia", "snapshot", "create",
		"--password", "secret-value",
		"--config-file", "/tmp/repository.config",
		"--password=another-secret",
	})
	want := []string{
		"kopia", "snapshot", "create",
		"--password", "<redacted>",
		"--config-file", "/tmp/repository.config",
		"--password=<redacted>",
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("sanitizeCommandForDisplay() = %v, want %v", got, want)
	}
}

func TestStripANSIRemovesControlCharacters(t *testing.T) {
	got := stripANSI("^D\b\bMailbox 3/47 \x1b[31mremote\x1b[0m")
	want := "Mailbox 3/47 remote"
	if got != want {
		t.Fatalf("stripANSI() = %q, want %q", got, want)
	}
}

func TestRenderAuditSummaryBoxBaselineInit(t *testing.T) {
	report := AuditReport{
		Summary: ReportSummary{
			Status:                "baseline-init",
			ImmutabilityDays:      30,
			BaselineUniqueMessages: 0,
			CurrentUniqueMessages:  60719,
		},
	}
	got := renderAuditSummaryBox(report)
	for _, needle := range []string{
		"+---------------- AUDIT ----------------+",
		"| Status     BASELINE INIT",
		"| Window     older than 30 days",
		"| Indexed    60,719 mails",
		"| New        60,719",
		"| Missing    0",
		"| Mutated    0",
		"| Moved/Lost 0",
		"+---------------------------------------+",
	} {
		if !strings.Contains(got, needle) {
			t.Fatalf("renderAuditSummaryBox() missing %q in:\n%s", needle, got)
		}
	}
}

func TestSetupPromptOptionalClearDoesNotReintroduceExistingS3Prefix(t *testing.T) {
	existing := map[string]string{
		"KOPIA_S3_PREFIX": "old-prefix/",
		"IMAP_HOST":       "imap.example.com",
	}
	values := map[string]string{
		"KOPIA_REPO_TYPE": "s3",
	}
	s3PrefixCleared := true

	for key, value := range existing {
		if _, ok := values[key]; ok {
			continue
		}
		if key == "KOPIA_S3_PREFIX" && s3PrefixCleared {
			continue
		}
		values[key] = value
	}

	if _, ok := values["KOPIA_S3_PREFIX"]; ok {
		t.Fatalf("values unexpectedly contains KOPIA_S3_PREFIX after explicit clear")
	}
	if values["IMAP_HOST"] != "imap.example.com" {
		t.Fatalf("other existing values should still be preserved")
	}
}

func TestRenderGeneratedKopiaPasswordNotice(t *testing.T) {
	got := renderGeneratedKopiaPasswordNotice("secret-value")
	for _, needle := range []string{
		"IMPORTANT: GENERATED KOPIA PASSWORD",
		"If you lose this password, your backups are effectively useless.",
		"Save it somewhere safe before continuing.",
		"KOPIA_PASSWORD=secret-value",
	} {
		if !strings.Contains(got, needle) {
			t.Fatalf("renderGeneratedKopiaPasswordNotice() missing %q in:\n%s", needle, got)
		}
	}
}

func TestNormalizedAccountTagKeepsReadableStableSlug(t *testing.T) {
	got := normalizedAccountTag("Example+Inbox@example.com")
	want := "example-inbox-example-com"
	if got != want {
		t.Fatalf("normalizedAccountTag() = %q, want %q", got, want)
	}
}

func TestChooseSnapshotScopeFallsBackAfterPrompt(t *testing.T) {
	restore := &RestoreApp{stdin: bufio.NewReader(strings.NewReader("y\n"))}
	allSnapshots := []kopiaSnapshot{{ID: "abcdef123456", StartTime: "2026-04-01T09:56:01Z"}}

	snapshots, scope, err := restore.chooseSnapshotScope(nil, allSnapshots)
	if err != nil {
		t.Fatalf("chooseSnapshotScope() error = %v", err)
	}
	if scope != snapshotScopeAllMailBackup {
		t.Fatalf("scope = %v, want snapshotScopeAllMailBackup", scope)
	}
	if len(snapshots) != 1 || snapshots[0].ID != "abcdef123456" {
		t.Fatalf("snapshots = %v", snapshots)
	}
}

func TestResolveKopiaPasswordPromptsForExistingPasswordWhenConnecting(t *testing.T) {
	app := &SetupApp{stdin: bufio.NewReader(strings.NewReader("existing-secret\n"))}
	got, err := app.resolveKopiaPassword("", "2")
	if err != nil {
		t.Fatalf("resolveKopiaPassword() error = %v", err)
	}
	if got != "existing-secret" {
		t.Fatalf("resolveKopiaPassword() = %q, want existing-secret", got)
	}
}

func TestResolveKopiaPasswordUsesExistingPasswordWhenConnecting(t *testing.T) {
	app := &SetupApp{stdin: bufio.NewReader(strings.NewReader("\n"))}
	got, err := app.resolveKopiaPassword("stored-secret", "2")
	if err != nil {
		t.Fatalf("resolveKopiaPassword() error = %v", err)
	}
	if got != "stored-secret" {
		t.Fatalf("resolveKopiaPassword() = %q, want stored-secret", got)
	}
}

func TestResolveKopiaPasswordAllowsOverridingStoredPasswordWhenConnecting(t *testing.T) {
	app := &SetupApp{stdin: bufio.NewReader(strings.NewReader("new-secret\n"))}
	got, err := app.resolveKopiaPassword("stored-secret", "2")
	if err != nil {
		t.Fatalf("resolveKopiaPassword() error = %v", err)
	}
	if got != "new-secret" {
		t.Fatalf("resolveKopiaPassword() = %q, want new-secret", got)
	}
}

func TestParseKopiaCompressionPolicyShow(t *testing.T) {
	output := strings.Join([]string{
		"Policy for test:",
		"",
		"Compression:",
		"  Compressor:                   zstd   inherited from (global)",
		"",
	}, "\n")
	if got := parseKopiaCompressionPolicyShow(output); got != "zstd   inherited from (global)" {
		t.Fatalf("parseKopiaCompressionPolicyShow() = %q", got)
	}
	if got := parseKopiaCompressionPolicyShow("Compression disabled.\n"); got != "disabled" {
		t.Fatalf("parseKopiaCompressionPolicyShow() disabled = %q", got)
	}
}

func TestRenderKopiaRepoStatus(t *testing.T) {
	status := kopiaRepoStatus{
		RepoType:                "s3",
		Storage:                 "s3://bucket/mail (s3.example.com)",
		Connected:               "yes",
		Encryption:              "configured",
		Compression:             "zstd",
		ComplianceHold:          "COMPLIANCE, 30 days",
		MaintenanceIntervalDays: 7,
		LastMaintenance:         "never",
		NextMaintenanceDue:      "now",
	}
	got := renderKopiaRepoStatus(status)
	for _, needle := range []string{
		"KOPIA REPOSITORY",
		"type: s3",
		"compression: zstd",
		"object lock / compliance hold: COMPLIANCE, 30 days",
		"maintenance: every 7 days",
	} {
		if !strings.Contains(got, needle) {
			t.Fatalf("renderKopiaRepoStatus() missing %q in:\n%s", needle, got)
		}
	}
}

func TestEnsureSetupToolsAvailable(t *testing.T) {
	if err := ensureSetupToolsAvailable([]string{}); err != nil {
		t.Fatalf("ensureSetupToolsAvailable(empty) error = %v", err)
	}
}

func manifestRecord(key string, messageTS, firstSeenTS, occurrences int, mailboxes, hashes []string) ManifestRecord {
	messageID := "<msg@example.com>"
	return ManifestRecord{
		Key:             key,
		MessageID:       &messageID,
		Subject:         "subject",
		MessageTS:       messageTS,
		FirstSeenTS:     firstSeenTS,
		Occurrences:     occurrences,
		Mailboxes:       mailboxes,
		SamplePaths:     []string{"cur/msg"},
		ContentHashes:   hashes,
		SizeBytes:       123,
		LatestFileMTime: 0,
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func regexpMustCompile(t *testing.T, value string) *regexp.Regexp {
	t.Helper()
	pattern, err := regexp.Compile(value)
	if err != nil {
		t.Fatalf("Compile(%s): %v", value, err)
	}
	return pattern
}
