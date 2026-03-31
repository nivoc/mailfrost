package internal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/mail"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	AlertExitCode                = 2
	currentManifestSchemaVersion = 2
	samplePathLimit              = 5
)

type Manifest struct {
	SchemaVersion      int                       `json:"schema_version"`
	GeneratedAt        string                    `json:"generated_at"`
	Maildir            string                    `json:"maildir"`
	IgnoreMailboxRegex string                    `json:"ignore_mailbox_regex"`
	Stats              ManifestStats             `json:"stats"`
	Records            map[string]ManifestRecord `json:"records"`
}

type ManifestStats struct {
	FilesScanned          int `json:"files_scanned"`
	FilesIgnoredByMailbox int `json:"files_ignored_by_mailbox"`
	ScanErrors            int `json:"scan_errors"`
	UniqueMessages        int `json:"unique_messages"`
}

type ManifestRecord struct {
	Key             string   `json:"key"`
	MessageID       *string  `json:"message_id"`
	Subject         string   `json:"subject"`
	MessageTS       int      `json:"message_ts"`
	FirstSeenTS     int      `json:"first_seen_ts"`
	Occurrences     int      `json:"occurrences"`
	Mailboxes       []string `json:"mailboxes"`
	SamplePaths     []string `json:"sample_paths"`
	ContentHashes   []string `json:"content_hashes"`
	SizeBytes       int64    `json:"size_bytes"`
	LatestFileMTime int64    `json:"latest_file_mtime"`
}

type mutableRecord struct {
	key             string
	messageID       *string
	subject         string
	messageTS       int
	firstSeenTS     int
	occurrences     int
	mailboxes       map[string]struct{}
	samplePaths     []string
	contentHashes   map[string]struct{}
	sizeBytes       int64
	latestFileMTime int64
}

type AuditReport struct {
	GeneratedAt string        `json:"generated_at"`
	Summary     ReportSummary `json:"summary"`
	Samples     ReportDetails `json:"samples"`
	Details     ReportDetails `json:"details"`
}

type ReportSummary struct {
	Status                      string `json:"status"`
	ImmutabilityDays            int    `json:"immutability_days"`
	BaselineUniqueMessages      int    `json:"baseline_unique_messages"`
	CurrentUniqueMessages       int    `json:"current_unique_messages"`
	MissingStableCount          int    `json:"missing_stable_count"`
	MutatedStableCount          int    `json:"mutated_stable_count"`
	PlacementChangedStableCount int    `json:"placement_changed_stable_count"`
}

type ReportDetails struct {
	MissingStableMessages          []MissingStableMessage  `json:"missing_stable_messages"`
	MutatedStableMessages          []MutatedStableMessage  `json:"mutated_stable_messages"`
	PlacementChangedStableMessages []PlacementChangeSample `json:"placement_changed_stable_messages"`
}

type MissingStableMessage struct {
	Key           string   `json:"key"`
	MessageID     *string  `json:"message_id"`
	Subject       string   `json:"subject"`
	MessageDate   string   `json:"message_date"`
	Mailboxes     []string `json:"mailboxes"`
	SamplePaths   []string `json:"sample_paths"`
	ContentHashes []string `json:"content_hashes"`
	Occurrences   int      `json:"occurrences"`
}

type MutatedStableMessage struct {
	Key            string   `json:"key"`
	MessageID      *string  `json:"message_id"`
	Subject        string   `json:"subject"`
	MessageDate    string   `json:"message_date"`
	Mailboxes      []string `json:"mailboxes"`
	BaselineHashes []string `json:"baseline_hashes"`
	CurrentHashes  []string `json:"current_hashes"`
	SamplePaths    []string `json:"sample_paths"`
	Occurrences    int      `json:"occurrences"`
}

type PlacementChangeSample struct {
	Key                 string   `json:"key"`
	MessageID           *string  `json:"message_id"`
	Subject             string   `json:"subject"`
	MessageDate         string   `json:"message_date"`
	BaselineMailboxes   []string `json:"baseline_mailboxes"`
	CurrentMailboxes    []string `json:"current_mailboxes"`
	BaselineOccurrences int      `json:"baseline_occurrences"`
	CurrentOccurrences  int      `json:"current_occurrences"`
	BaselinePaths       []string `json:"baseline_paths"`
	CurrentPaths        []string `json:"current_paths"`
}

func BuildManifest(maildirRoot, ignoreMailboxRegex string, observedAt time.Time) (Manifest, error) {
	info, err := os.Stat(maildirRoot)
	if err != nil || !info.IsDir() {
		return Manifest{}, fmt.Errorf("maildir_path does not exist or is not a directory: %s", maildirRoot)
	}

	ignorePattern, err := regexp.Compile(ignoreMailboxRegex)
	if err != nil {
		return Manifest{}, fmt.Errorf("compile ignore_mailbox_regex: %w", err)
	}

	records := map[string]*mutableRecord{}
	stats := ManifestStats{}
	var scanErrors []string

	err = filepath.WalkDir(maildirRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			scanErrors = append(scanErrors, fmt.Sprintf("%s: %v", relativePath(maildirRoot, path), walkErr))
			stats.ScanErrors++
			return nil
		}
		if d.IsDir() {
			name := d.Name()
			if name == "tmp" {
				return filepath.SkipDir
			}
			return nil
		}

		parent := filepath.Base(filepath.Dir(path))
		if parent != "cur" && parent != "new" {
			return nil
		}

		mailbox := mailboxName(maildirRoot, path)
		if ignorePattern.String() != "" && ignorePattern.MatchString(mailbox) {
			stats.FilesIgnoredByMailbox++
			return nil
		}

		stats.FilesScanned++
		record, parseErr := manifestRecordForMessage(maildirRoot, path, observedAt)
		if parseErr != nil {
			scanErrors = append(scanErrors, fmt.Sprintf("%s: %v", relativePath(maildirRoot, path), parseErr))
			stats.ScanErrors++
			return nil
		}

		existing := records[record.key]
		if existing == nil {
			records[record.key] = record
			return nil
		}

		existing.occurrences++
		existing.mailboxes[mailbox] = struct{}{}
		for hash := range record.contentHashes {
			existing.contentHashes[hash] = struct{}{}
		}
		if record.sizeBytes > existing.sizeBytes {
			existing.sizeBytes = record.sizeBytes
		}
		if record.latestFileMTime > existing.latestFileMTime {
			existing.latestFileMTime = record.latestFileMTime
		}
		if record.messageTS < existing.messageTS {
			existing.messageTS = record.messageTS
		}
		if existing.messageID == nil && record.messageID != nil {
			existing.messageID = record.messageID
		}
		if existing.subject == "" && record.subject != "" {
			existing.subject = record.subject
		}
		for _, relPath := range record.samplePaths {
			if !contains(existing.samplePaths, relPath) && len(existing.samplePaths) < samplePathLimit {
				existing.samplePaths = append(existing.samplePaths, relPath)
			}
		}
		return nil
	})
	if err != nil {
		return Manifest{}, err
	}

	if len(scanErrors) > 0 {
		sample := strings.Join(scanErrors[:min(5, len(scanErrors))], "; ")
		return Manifest{}, fmt.Errorf("could not read %d Maildir files while building the manifest. Sample: %s", len(scanErrors), sample)
	}

	frozenRecords := map[string]ManifestRecord{}
	for key, record := range records {
		frozenRecords[key] = ManifestRecord{
			Key:             record.key,
			MessageID:       record.messageID,
			Subject:         record.subject,
			MessageTS:       record.messageTS,
			FirstSeenTS:     record.firstSeenTS,
			Occurrences:     record.occurrences,
			Mailboxes:       sortedKeys(record.mailboxes),
			SamplePaths:     append([]string(nil), record.samplePaths...),
			ContentHashes:   sortedKeys(record.contentHashes),
			SizeBytes:       record.sizeBytes,
			LatestFileMTime: record.latestFileMTime,
		}
	}
	stats.UniqueMessages = len(frozenRecords)

	return Manifest{
		SchemaVersion:      currentManifestSchemaVersion,
		GeneratedAt:        utcNow().Format(time.RFC3339),
		Maildir:            maildirRoot,
		IgnoreMailboxRegex: ignoreMailboxRegex,
		Stats:              stats,
		Records:            frozenRecords,
	}, nil
}

func manifestRecordForMessage(maildirRoot, messagePath string, observedAt time.Time) (*mutableRecord, error) {
	rawMessage, err := os.ReadFile(messagePath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(messagePath)
	if err != nil {
		return nil, err
	}

	msg, err := mail.ReadMessage(bytes.NewReader(rawMessage))
	if err != nil {
		return nil, err
	}
	messageHash := sha256HexBytes(rawMessage)
	normalized := normalizeMessageID(msg.Header.Get("Message-ID"))
	subject := cleanText(msg.Header.Get("Subject"))
	messageTS := parseMessageTimestamp(msg.Header.Get("Date"), info.ModTime())
	key := "sha256:" + messageHash
	if normalized != nil {
		key = "mid:" + *normalized
	}
	mailbox := mailboxName(maildirRoot, messagePath)
	relPath := relativePath(maildirRoot, messagePath)
	firstSeenTS := messageTS
	observedEpoch := int(observedAt.Unix())
	if firstSeenTS > observedEpoch {
		firstSeenTS = observedEpoch
	}

	return &mutableRecord{
		key:             key,
		messageID:       normalized,
		subject:         subject,
		messageTS:       messageTS,
		firstSeenTS:     firstSeenTS,
		occurrences:     1,
		mailboxes:       map[string]struct{}{mailbox: {}},
		samplePaths:     []string{relPath},
		contentHashes:   map[string]struct{}{messageHash: {}},
		sizeBytes:       info.Size(),
		latestFileMTime: info.ModTime().Unix(),
	}, nil
}

func LoadManifest(path string) (Manifest, error) {
	if err := validateManifestChecksum(path); err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	if err := validateManifest(path, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(path string, manifest Manifest) error {
	if manifest.SchemaVersion != 1 && manifest.SchemaVersion != currentManifestSchemaVersion {
		return fmt.Errorf("unsupported manifest schema_version in %s: %d", path, manifest.SchemaVersion)
	}
	if manifest.Records == nil {
		return fmt.Errorf("manifest records must be present: %s", path)
	}
	for key, record := range manifest.Records {
		if record.Key != key {
			return fmt.Errorf("manifest record key mismatch for %s in %s", key, path)
		}
		if record.Occurrences < 1 {
			return fmt.Errorf("manifest occurrences must be >= 1 for %s in %s", key, path)
		}
		if record.SizeBytes < 0 {
			return fmt.Errorf("manifest size_bytes must be >= 0 for %s in %s", key, path)
		}
		if record.LatestFileMTime < 0 {
			return fmt.Errorf("manifest latest_file_mtime must be >= 0 for %s in %s", key, path)
		}
	}
	return nil
}

func MergeManifestHistory(previous Manifest, current Manifest) Manifest {
	for key, currentRecord := range current.Records {
		previousRecord, ok := previous.Records[key]
		if !ok {
			continue
		}
		if previousRecord.FirstSeenTS != 0 {
			currentRecord.FirstSeenTS = previousRecord.FirstSeenTS
		} else if previousRecord.MessageTS != 0 && previousRecord.MessageTS < currentRecord.FirstSeenTS {
			currentRecord.FirstSeenTS = previousRecord.MessageTS
		}
		current.Records[key] = currentRecord
	}
	current.SchemaVersion = currentManifestSchemaVersion
	return current
}

func CompareManifests(baseline, current Manifest, immutabilityDays, sampleLimit int, now time.Time) AuditReport {
	cutoffEpoch := int(now.Add(-time.Duration(immutabilityDays) * 24 * time.Hour).Unix())
	missing := []MissingStableMessage{}
	mutated := []MutatedStableMessage{}
	placementChanges := []PlacementChangeSample{}

	for key, baselineRecord := range baseline.Records {
		if !stableRecord(baselineRecord, cutoffEpoch) {
			continue
		}
		currentRecord, ok := current.Records[key]
		if !ok {
			missing = append(missing, sampleMissingItem(baselineRecord))
			continue
		}
		if !stringSlicesEqual(baselineRecord.ContentHashes, currentRecord.ContentHashes) {
			mutated = append(mutated, sampleMutatedItem(baselineRecord, currentRecord))
		}
		if !stringSlicesEqual(baselineRecord.Mailboxes, currentRecord.Mailboxes) || baselineRecord.Occurrences != currentRecord.Occurrences {
			placementChanges = append(placementChanges, samplePlacementChangeItem(baselineRecord, currentRecord))
		}
	}

	status := "ok"
	if len(missing) > 0 || len(mutated) > 0 || len(placementChanges) > 0 {
		status = "alert"
	}

	return AuditReport{
		GeneratedAt: utcNow().Format(time.RFC3339),
		Summary: ReportSummary{
			Status:                      status,
			ImmutabilityDays:            immutabilityDays,
			BaselineUniqueMessages:      len(baseline.Records),
			CurrentUniqueMessages:       len(current.Records),
			MissingStableCount:          len(missing),
			MutatedStableCount:          len(mutated),
			PlacementChangedStableCount: len(placementChanges),
		},
		Samples: ReportDetails{
			MissingStableMessages:          truncateMissing(missing, sampleLimit),
			MutatedStableMessages:          truncateMutated(mutated, sampleLimit),
			PlacementChangedStableMessages: truncatePlacement(placementChanges, sampleLimit),
		},
		Details: ReportDetails{
			MissingStableMessages:          missing,
			MutatedStableMessages:          mutated,
			PlacementChangedStableMessages: placementChanges,
		},
	}
}

func BaselineInitReport(immutabilityDays, currentUniqueMessages int) AuditReport {
	return AuditReport{
		GeneratedAt: utcNow().Format(time.RFC3339),
		Summary: ReportSummary{
			Status:                      "baseline-init",
			ImmutabilityDays:            immutabilityDays,
			BaselineUniqueMessages:      0,
			CurrentUniqueMessages:       currentUniqueMessages,
			MissingStableCount:          0,
			MutatedStableCount:          0,
			PlacementChangedStableCount: 0,
		},
		Samples: ReportDetails{},
		Details: ReportDetails{},
	}
}

func RenderReportText(report AuditReport, sampleLimit int) string {
	lines := []string{
		fmt.Sprintf("Status: %s", strings.ToUpper(report.Summary.Status)),
		fmt.Sprintf("Stable messages missing: %d", report.Summary.MissingStableCount),
		fmt.Sprintf("Stable messages mutated: %d", report.Summary.MutatedStableCount),
		fmt.Sprintf("Stable messages with mailbox/count changes: %d", report.Summary.PlacementChangedStableCount),
		fmt.Sprintf("Immutability window (days): %d", report.Summary.ImmutabilityDays),
		fmt.Sprintf("Baseline unique messages: %d", report.Summary.BaselineUniqueMessages),
		fmt.Sprintf("Current unique messages: %d", report.Summary.CurrentUniqueMessages),
	}

	if report.Summary.Status == "baseline-init" {
		lines = append(lines, "", "No previous baseline exists. The current Maildir state will become the baseline after a successful backup.")
	}
	if len(report.Samples.MissingStableMessages) > 0 {
		lines = append(lines, "", "Missing samples:")
		for _, item := range report.Samples.MissingStableMessages {
			lines = append(lines, fmt.Sprintf("- %s | %s | %s | mailboxes=%s | paths=%s", item.MessageDate, reportSubject(item.Subject), reportMessageID(item.MessageID, item.Key), strings.Join(item.Mailboxes, ", "), strings.Join(item.SamplePaths, ", ")))
		}
	}
	if len(report.Samples.MutatedStableMessages) > 0 {
		lines = append(lines, "", "Mutated samples:")
		for _, item := range report.Samples.MutatedStableMessages {
			lines = append(lines, fmt.Sprintf("- %s | %s | %s | mailboxes=%s | paths=%s", item.MessageDate, reportSubject(item.Subject), reportMessageID(item.MessageID, item.Key), strings.Join(item.Mailboxes, ", "), strings.Join(item.SamplePaths, ", ")))
		}
	}
	if len(report.Samples.PlacementChangedStableMessages) > 0 {
		lines = append(lines, "", "Mailbox/count change samples:")
		for _, item := range report.Samples.PlacementChangedStableMessages {
			lines = append(lines, fmt.Sprintf("- %s | %s | %s | baseline_mailboxes=%s | current_mailboxes=%s | baseline_occurrences=%d | current_occurrences=%d", item.MessageDate, reportSubject(item.Subject), reportMessageID(item.MessageID, item.Key), strings.Join(item.BaselineMailboxes, ", "), strings.Join(item.CurrentMailboxes, ", "), item.BaselineOccurrences, item.CurrentOccurrences))
		}
	}

	truncated := false
	if len(report.Details.MissingStableMessages) > sampleLimit {
		lines = append(lines, fmt.Sprintf("  ... %d more missing messages not shown", len(report.Details.MissingStableMessages)-sampleLimit))
		truncated = true
	}
	if len(report.Details.MutatedStableMessages) > sampleLimit {
		lines = append(lines, fmt.Sprintf("  ... %d more mutated messages not shown", len(report.Details.MutatedStableMessages)-sampleLimit))
		truncated = true
	}
	if len(report.Details.PlacementChangedStableMessages) > sampleLimit {
		lines = append(lines, fmt.Sprintf("  ... %d more mailbox/count changes not shown", len(report.Details.PlacementChangedStableMessages)-sampleLimit))
		truncated = true
	}
	if truncated {
		lines = append(lines, "", "Full details in the JSON report (see 'details' key).")
	}

	return strings.Join(lines, "\n") + "\n"
}

func WriteJSON(path string, payload any) error {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func WriteText(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func WriteManifestFile(path string, payload Manifest) error {
	if err := WriteJSON(path, payload); err != nil {
		return err
	}
	return writeChecksum(path)
}

func checksumPath(path string) string {
	return path + ".sha256"
}

func writeChecksum(path string) error {
	digest, err := fileSHA256(path)
	if err != nil {
		return err
	}
	return os.WriteFile(checksumPath(path), []byte(digest+"\n"), 0o644)
}

func validateManifestChecksum(path string) error {
	checksumFile := checksumPath(path)
	if _, err := os.Stat(checksumFile); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	expectedBytes, err := os.ReadFile(checksumFile)
	if err != nil {
		return err
	}
	expected := strings.TrimSpace(strings.ToLower(string(expectedBytes)))
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(expected) {
		return fmt.Errorf("invalid checksum file for manifest: %s", checksumFile)
	}
	actual, err := fileSHA256(path)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("manifest checksum mismatch: %s", path)
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return sha256HexBytes(data), nil
}

func stableRecord(record ManifestRecord, cutoffEpoch int) bool {
	if record.FirstSeenTS != 0 || record.MessageTS > cutoffEpoch {
		return record.FirstSeenTS <= cutoffEpoch
	}
	return record.MessageTS <= cutoffEpoch
}

func sampleMissingItem(record ManifestRecord) MissingStableMessage {
	return MissingStableMessage{
		Key:           record.Key,
		MessageID:     record.MessageID,
		Subject:       record.Subject,
		MessageDate:   formatEpoch(record.MessageTS),
		Mailboxes:     append([]string(nil), record.Mailboxes...),
		SamplePaths:   append([]string(nil), record.SamplePaths...),
		ContentHashes: append([]string(nil), record.ContentHashes...),
		Occurrences:   record.Occurrences,
	}
}

func sampleMutatedItem(baselineRecord, currentRecord ManifestRecord) MutatedStableMessage {
	subject := baselineRecord.Subject
	if subject == "" {
		subject = currentRecord.Subject
	}
	return MutatedStableMessage{
		Key:            baselineRecord.Key,
		MessageID:      baselineRecord.MessageID,
		Subject:        subject,
		MessageDate:    formatEpoch(baselineRecord.MessageTS),
		Mailboxes:      append([]string(nil), currentRecord.Mailboxes...),
		BaselineHashes: append([]string(nil), baselineRecord.ContentHashes...),
		CurrentHashes:  append([]string(nil), currentRecord.ContentHashes...),
		SamplePaths:    append([]string(nil), currentRecord.SamplePaths...),
		Occurrences:    currentRecord.Occurrences,
	}
}

func samplePlacementChangeItem(baselineRecord, currentRecord ManifestRecord) PlacementChangeSample {
	subject := baselineRecord.Subject
	if subject == "" {
		subject = currentRecord.Subject
	}
	return PlacementChangeSample{
		Key:                 baselineRecord.Key,
		MessageID:           baselineRecord.MessageID,
		Subject:             subject,
		MessageDate:         formatEpoch(baselineRecord.MessageTS),
		BaselineMailboxes:   append([]string(nil), baselineRecord.Mailboxes...),
		CurrentMailboxes:    append([]string(nil), currentRecord.Mailboxes...),
		BaselineOccurrences: baselineRecord.Occurrences,
		CurrentOccurrences:  currentRecord.Occurrences,
		BaselinePaths:       append([]string(nil), baselineRecord.SamplePaths...),
		CurrentPaths:        append([]string(nil), currentRecord.SamplePaths...),
	}
}

func normalizeMessageID(value string) *string {
	compact := strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	if compact == "" {
		return nil
	}
	if strings.HasPrefix(compact, "<") && strings.HasSuffix(compact, ">") {
		compact = strings.TrimSpace(compact[1 : len(compact)-1])
	}
	compact = strings.ToLower(compact)
	if compact == "" {
		return nil
	}
	normalized := "<" + compact + ">"
	return &normalized
}

func cleanText(value string) string {
	if value == "" {
		return ""
	}
	return strings.Join(strings.Fields(value), " ")
}

func parseMessageTimestamp(headerValue string, fallback time.Time) int {
	if strings.TrimSpace(headerValue) == "" {
		return int(fallback.Unix())
	}
	parsed, err := mail.ParseDate(headerValue)
	if err != nil {
		return int(fallback.Unix())
	}
	return int(parsed.Unix())
}

func mailboxName(maildirRoot, messagePath string) string {
	mailboxRoot := filepath.Dir(filepath.Dir(messagePath))
	relative, err := filepath.Rel(maildirRoot, mailboxRoot)
	if err != nil || relative == "." {
		return "INBOX"
	}
	return filepath.ToSlash(relative)
}

func relativePath(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relative)
}

func formatEpoch(epoch int) string {
	if epoch == 0 {
		return "unknown"
	}
	return time.Unix(int64(epoch), 0).UTC().Format("2006-01-02")
}

func sha256HexBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sortedKeys(values map[string]struct{}) []string {
	items := make([]string, 0, len(values))
	for item := range values {
		items = append(items, item)
	}
	sort.Strings(items)
	return items
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for i := range leftCopy {
		if leftCopy[i] != rightCopy[i] {
			return false
		}
	}
	return true
}

func reportSubject(subject string) string {
	if subject == "" {
		return "(no subject)"
	}
	return subject
}

func reportMessageID(messageID *string, fallback string) string {
	if messageID != nil && *messageID != "" {
		return *messageID
	}
	return fallback
}

func truncateMissing(items []MissingStableMessage, limit int) []MissingStableMessage {
	return items[:min(limit, len(items))]
}

func truncateMutated(items []MutatedStableMessage, limit int) []MutatedStableMessage {
	return items[:min(limit, len(items))]
}

func truncatePlacement(items []PlacementChangeSample, limit int) []PlacementChangeSample {
	return items[:min(limit, len(items))]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
