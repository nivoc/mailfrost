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
	app := &SetupApp{stdin: bufio.NewReader(strings.NewReader("-\n"))}
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
	got := app.buildKopiaCompressionPolicyCommand("/tmp/repository.config", "secret", "/tmp/maildir")
	want := []string{
		"kopia", "policy", "set",
		"--config-file", "/tmp/repository.config",
		"--password", "secret",
		"/tmp/maildir",
		"--compression", "zstd",
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("buildKopiaCompressionPolicyCommand() = %v, want %v", got, want)
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
