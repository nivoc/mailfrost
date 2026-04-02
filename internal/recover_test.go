package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-imap"
)

func TestCollectRecoverMaildirIncludesFlagsAndIgnoresConfiguredMailboxes(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "INBOX", "cur", "seen:2,S"), sampleRecoverMessage("inbox"))
	mustWriteFile(t, filepath.Join(root, ".Archive", "new", "plain"), sampleRecoverMessage("archive"))
	mustWriteFile(t, filepath.Join(root, ".Spam", "cur", "junk:2,S"), sampleRecoverMessage("spam"))

	mailboxes, err := collectRecoverMaildir(root, defaultIgnoreMailboxRegex)
	if err != nil {
		t.Fatalf("collectRecoverMaildir() error = %v", err)
	}
	if len(mailboxes) != 2 {
		t.Fatalf("len(mailboxes) = %d, want 2", len(mailboxes))
	}
	if mailboxes[0].Name != ".Archive" {
		t.Fatalf("mailboxes[0].Name = %s, want .Archive", mailboxes[0].Name)
	}
	if mailboxes[1].Name != "INBOX" {
		t.Fatalf("mailboxes[1].Name = %s, want INBOX", mailboxes[1].Name)
	}
	if mailboxes[1].MessageCount != 1 {
		t.Fatalf("mailboxes[1].MessageCount = %d, want 1", mailboxes[1].MessageCount)
	}
}

func TestCollectRecoverMaildirKeepsEmptySnapshotMailboxes(t *testing.T) {
	root := t.TempDir()
	for _, mailbox := range []string{"Archive", "INBOX", "Sent"} {
		for _, child := range []string{"cur", "new", "tmp"} {
			path := filepath.Join(root, mailbox, child)
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatalf("MkdirAll(%s): %v", path, err)
			}
		}
	}
	mustWriteFile(t, filepath.Join(root, "INBOX", "cur", "seen:2,S"), sampleRecoverMessage("inbox"))

	mailboxes, err := collectRecoverMaildir(root, defaultIgnoreMailboxRegex)
	if err != nil {
		t.Fatalf("collectRecoverMaildir() error = %v", err)
	}
	if len(mailboxes) != 3 {
		t.Fatalf("len(mailboxes) = %d, want 3", len(mailboxes))
	}
	if mailboxes[0].Name != "Archive" || mailboxes[0].MessageCount != 0 {
		t.Fatalf("mailboxes[0] = %+v, want empty Archive mailbox", mailboxes[0])
	}
	if mailboxes[2].Name != "Sent" || mailboxes[2].MessageCount != 0 {
		t.Fatalf("mailboxes[2] = %+v, want empty Sent mailbox", mailboxes[2])
	}
}

func TestRecoverBuildPlanSeparatesCreateClearAndDeleteWork(t *testing.T) {
	app := &RecoverApp{
		Config: Config{
			IgnoreMailboxRegex: defaultIgnoreMailboxRegex,
		},
	}

	desired := []recoverMailbox{
		{Name: ".Archive", MessageCount: 2},
		{Name: "INBOX", MessageCount: 1},
	}

	store := fakeRecoverStore{
		mailboxes: []recoverRemoteMailbox{
			{Name: ".Archive", MessageCount: 5, Attributes: []string{imap.ArchiveAttr}},
			{Name: ".Old", MessageCount: 2},
			{Name: "INBOX", MessageCount: 4},
			{Name: ".Spam", MessageCount: 8},
		},
	}

	plan, err := app.buildPlan(&store, kopiaSnapshot{ID: "snapshot-123"}, "/tmp/staging", desired)
	if err != nil {
		t.Fatalf("buildPlan() error = %v", err)
	}
	if plan.UploadMessageCount != 3 {
		t.Fatalf("UploadMessageCount = %d, want 3", plan.UploadMessageCount)
	}
	if plan.DeleteMessageCount != 11 {
		t.Fatalf("DeleteMessageCount = %d, want 11", plan.DeleteMessageCount)
	}
	if plan.SafetyMailboxRoot != "Recovery-Safety-manual" {
		t.Fatalf("SafetyMailboxRoot = %s, want Recovery-Safety-manual", plan.SafetyMailboxRoot)
	}
	if got := strings.Join(plan.DeleteMailboxes, ","); got != ".Old" {
		t.Fatalf("DeleteMailboxes = %s, want .Old", got)
	}
	if len(plan.CreateMailboxes) != 0 {
		t.Fatalf("CreateMailboxes = %v, want empty", plan.CreateMailboxes)
	}
	if len(plan.ClearMailboxes) != 3 {
		t.Fatalf("len(ClearMailboxes) = %d, want 3", len(plan.ClearMailboxes))
	}
}

type fakeRecoverStore struct {
	mailboxes []recoverRemoteMailbox
}

func (f *fakeRecoverStore) ListManagedMailboxes(ignorePattern *regexp.Regexp) ([]recoverRemoteMailbox, error) {
	var filtered []recoverRemoteMailbox
	for _, mailbox := range f.mailboxes {
		if recoverMailboxIgnored(mailbox.Name, ignorePattern) {
			continue
		}
		filtered = append(filtered, mailbox)
	}
	return filtered, nil
}

func (f *fakeRecoverStore) CreateMailbox(name string) error {
	return nil
}

func (f *fakeRecoverStore) CopyMailboxMessages(sourceMailbox, targetMailbox string) (int, error) {
	return 0, nil
}

func (f *fakeRecoverStore) ClearMailbox(name string) (int, error) {
	return 0, nil
}

func (f *fakeRecoverStore) DeleteMailbox(name string) error {
	return nil
}

func (f *fakeRecoverStore) Close() error {
	return nil
}

func sampleRecoverMessage(subject string) string {
	return strings.Join([]string{
		"From: sender@example.com",
		"To: receiver@example.com",
		"Date: Tue, 31 Mar 2026 12:00:00 +0000",
		"Message-ID: <" + subject + "@example.com>",
		"Subject: " + subject,
		"",
		"body",
		"",
	}, "\n")
}

func TestRecoverMailboxDeletableRejectsSpecialUse(t *testing.T) {
	if recoverMailboxDeletable(recoverRemoteMailbox{Name: "Archive", Attributes: []string{imap.ArchiveAttr}}) {
		t.Fatalf("Archive mailbox should not be deletable")
	}
	if !recoverMailboxDeletable(recoverRemoteMailbox{Name: "Project"}) {
		t.Fatalf("regular mailbox should be deletable")
	}
}

func TestRenderRecoveryMbsyncConfigUsesPushAndIsolatedSyncState(t *testing.T) {
	got := renderRecoveryMbsyncConfig("imap.fastmail.com", "993", "testing@mk1.me", "/tmp/staging", "/tmp/syncstate", recoveryMbsyncPatterns(defaultIgnoreMailboxRegex))
	for _, needle := range []string{
		"Sync Push",
		"Create Far",
		"CopyArrivalDate yes",
		`SyncState "/tmp/syncstate/"`,
		`Path "/tmp/staging/"`,
		`Inbox "/tmp/staging/INBOX"`,
		"Patterns INBOX *",
		"!Recovery-Safety-*",
		"!Trash",
		"!Spam",
		"!Drafts",
		"!Junk",
	} {
		if !strings.Contains(got, needle) {
			t.Fatalf("renderRecoveryMbsyncConfig() missing %q in:\n%s", needle, got)
		}
	}
}

func TestRecoveryMbsyncPatternsDefaultIgnoreExcludesUntouchedFolders(t *testing.T) {
	got := recoveryMbsyncPatterns(defaultIgnoreMailboxRegex)
	want := []string{
		"INBOX", "*", "!Recovery-Safety-*",
		"!Trash", "!Trash/*",
		"!Junk", "!Junk/*",
		"!Spam", "!Spam/*",
		"!Drafts", "!Drafts/*",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("recoveryMbsyncPatterns() = %v, want %v", got, want)
	}
}

func TestRecoverySafetyMailboxNaming(t *testing.T) {
	if got := recoverySafetyMailboxRoot("20260331T151410Z"); got != "Recovery-Safety-20260331T151410" {
		t.Fatalf("recoverySafetyMailboxRoot() = %s", got)
	}
	if got := recoverySafetyMailboxName("Recovery-Safety-20260331T151410", "INBOX"); got != "Recovery-Safety-20260331T151410/INBOX" {
		t.Fatalf("recoverySafetyMailboxName(INBOX) = %s", got)
	}
	if got := recoverySafetyMailboxName("Recovery-Safety-20260331T151410", "Archive"); got != "Recovery-Safety-20260331T151410/Archive" {
		t.Fatalf("recoverySafetyMailboxName(Archive) = %s", got)
	}
}

func TestRecoverMailboxIgnoredExcludesSafetyTree(t *testing.T) {
	pattern := regexp.MustCompile(defaultIgnoreMailboxRegex)
	if !recoverMailboxIgnored("Recovery-Safety-20260331T151410", pattern) {
		t.Fatalf("expected safety root to be ignored")
	}
	if !recoverMailboxIgnored("Recovery-Safety-20260331T151410/INBOX", pattern) {
		t.Fatalf("expected safety child mailbox to be ignored")
	}
	if !recoverMailboxIgnored("Trash", pattern) {
		t.Fatalf("expected Trash to be ignored by regex")
	}
	if recoverMailboxIgnored("INBOX", pattern) {
		t.Fatalf("did not expect INBOX to be ignored")
	}
}

func TestNormalizeRecoveryMaildirTimesUsesMessageDate(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "INBOX", "cur", "msg:2,S")
	mustWriteFile(t, path, sampleRecoverMessage("dated"))
	now := time.Unix(1774963819, 0)
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatalf("Chtimes(%s): %v", path, err)
	}

	if err := normalizeRecoveryMaildirTimes(root, defaultIgnoreMailboxRegex); err != nil {
		t.Fatalf("normalizeRecoveryMaildirTimes() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%s): %v", path, err)
	}
	got := info.ModTime().UTC().Format(time.RFC3339)
	want := "2026-03-31T12:00:00Z"
	if got != want {
		t.Fatalf("mtime = %s, want %s", got, want)
	}
}

func TestIsRecoverStoreRetryable(t *testing.T) {
	for _, errText := range []string{
		"write: broken pipe",
		"read tcp: connection reset by peer",
		"unexpected EOF",
		"use of closed network connection",
	} {
		if !isRecoverStoreRetryable(fmt.Errorf("%s", errText)) {
			t.Fatalf("expected %q to be retryable", errText)
		}
	}
	if isRecoverStoreRetryable(fmt.Errorf("invalid credentials")) {
		t.Fatalf("did not expect unrelated IMAP error to be retryable")
	}
}

func TestFindLatestRecoveryStagingReturnsNewestMatchingSnapshot(t *testing.T) {
	root := t.TempDir()
	older := filepath.Join(root, "snapshot-abcdef1234-older")
	newer := filepath.Join(root, "snapshot-abcdef1234-newer")
	other := filepath.Join(root, "snapshot-ffffffffff-other")
	for _, path := range []string{older, newer, other} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", path, err)
		}
	}
	oldTime := time.Unix(100, 0)
	newTime := time.Unix(200, 0)
	if err := os.Chtimes(older, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes(%s): %v", older, err)
	}
	if err := os.Chtimes(newer, newTime, newTime); err != nil {
		t.Fatalf("Chtimes(%s): %v", newer, err)
	}

	got, err := findLatestRecoveryStaging(root, "abcdef1234")
	if err != nil {
		t.Fatalf("findLatestRecoveryStaging() error = %v", err)
	}
	if got != newer {
		t.Fatalf("findLatestRecoveryStaging() = %s, want %s", got, newer)
	}
}

func TestFindRecoveryResumeConfigReturnsNewestConfigWhenRunIDMissing(t *testing.T) {
	root := t.TempDir()
	older := filepath.Join(root, "mbsyncrc.recover.20260401T100000Z")
	newer := filepath.Join(root, "mbsyncrc.recover.20260401T110000Z")
	mustWriteFile(t, older, "older")
	mustWriteFile(t, newer, "newer")
	oldTime := time.Unix(100, 0)
	newTime := time.Unix(200, 0)
	if err := os.Chtimes(older, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes(%s): %v", older, err)
	}
	if err := os.Chtimes(newer, newTime, newTime); err != nil {
		t.Fatalf("Chtimes(%s): %v", newer, err)
	}

	configPath, runID, err := findRecoveryResumeConfig(root, "")
	if err != nil {
		t.Fatalf("findRecoveryResumeConfig() error = %v", err)
	}
	if configPath != newer {
		t.Fatalf("configPath = %s, want %s", configPath, newer)
	}
	if runID != "20260401T110000Z" {
		t.Fatalf("runID = %s", runID)
	}
}

func TestFindRecoveryResumeConfigReturnsExplicitRunID(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "mbsyncrc.recover.20260401T141455Z")
	mustWriteFile(t, path, "config")

	configPath, runID, err := findRecoveryResumeConfig(root, "20260401T141455Z")
	if err != nil {
		t.Fatalf("findRecoveryResumeConfig() error = %v", err)
	}
	if configPath != path {
		t.Fatalf("configPath = %s, want %s", configPath, path)
	}
	if runID != "20260401T141455Z" {
		t.Fatalf("runID = %s", runID)
	}
}
