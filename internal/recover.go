package internal

import (
	"bufio"
	"bytes"
	"fmt"
	"net/mail"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
)

type RecoverApp struct {
	Config          Config
	Runtime         *Runtime
	SnapshotFlag    string
	YesFlag         bool
	ConfirmUserFlag string
	SafetyCopy      bool
	stdin           *bufio.Reader
}

type recoverMailbox struct {
	Name         string
	MessageCount int
}

type recoverRemoteMailbox struct {
	Name         string
	MessageCount int
	Attributes   []string
}

type recoverPlan struct {
	Snapshot            kopiaSnapshot
	StagingPath         string
	SafetyMailboxRoot   string
	SafetyCopyEnabled   bool
	DesiredMailboxes    []recoverMailbox
	ManagedRemote       []recoverRemoteMailbox
	CreateMailboxes     []string
	ClearMailboxes      []recoverRemoteMailbox
	DeleteMailboxes     []string
	UploadMessageCount  int
	DeleteMessageCount  int
	ManagedMailboxCount int
}

type recoverStore interface {
	ListManagedMailboxes(ignorePattern *regexp.Regexp) ([]recoverRemoteMailbox, error)
	CreateMailbox(name string) error
	CopyMailboxMessages(sourceMailbox, targetMailbox string) (int, error)
	ClearMailbox(name string) (int, error)
	DeleteMailbox(name string) error
	Close() error
}

func (a *RecoverApp) Run() error {
	a.stdin = bufio.NewReader(os.Stdin)

	restoreApp := &RestoreApp{
		Config:       a.Config,
		Runtime:      a.Runtime,
		SnapshotFlag: a.SnapshotFlag,
		stdin:        a.stdin,
	}

	a.Runtime.Console("Loading snapshots...")
	snapshots, scope, err := restoreApp.loadSnapshotsForSelection(10)
	if err != nil {
		return err
	}
	if len(snapshots) == 0 {
		a.Runtime.Console("No snapshots found")
		return nil
	}

	var selected kopiaSnapshot
	if a.SnapshotFlag != "" {
		selected, err = restoreApp.findSnapshot(a.SnapshotFlag)
	} else {
		selected, err = restoreApp.promptSnapshot(snapshots, scope)
	}
	if err != nil {
		return err
	}

	stagingRoot := filepath.Join(a.Config.StateDir, "recoveries")
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		return fmt.Errorf("create recovery staging dir: %w", err)
	}
	stagingPath, restoredFresh, err := a.prepareRecoveryStaging(restoreApp, selected, stagingRoot)
	if err != nil {
		return err
	}
	if !restoredFresh {
		a.Runtime.Console(fmt.Sprintf("Reusing existing staged snapshot at %s", stagingPath))
	}

	desiredMailboxes, err := collectRecoverMaildir(stagingPath, a.Config.IgnoreMailboxRegex)
	if err != nil {
		return err
	}
	if err := normalizeRecoveryMaildirTimes(stagingPath, a.Config.IgnoreMailboxRegex); err != nil {
		return err
	}

	store, err := connectRecoverStore(a.Config)
	if err != nil {
		return err
	}

	plan, err := a.buildPlan(store, selected, stagingPath, desiredMailboxes)
	_ = store.Close()
	if err != nil {
		return err
	}
	if a.YesFlag {
		a.SafetyCopy = true
		plan.SafetyCopyEnabled = true
	} else {
		if err := a.chooseSafetyCopy(&plan); err != nil {
			return err
		}
	}
	a.printPlan(plan)
	if err := a.confirm(plan); err != nil {
		return err
	}
	if err := a.executePlan(plan); err != nil {
		return err
	}
	if err := a.verifyPlan(plan); err != nil {
		return err
	}
	if err := a.postRecoveryCleanup(plan); err != nil {
		return err
	}

	a.Runtime.Console(fmt.Sprintf("Recovery completed for snapshot %s", selected.ShortID()))
	if _, err := os.Stat(stagingPath); err == nil {
		a.Runtime.Console(fmt.Sprintf("Staged snapshot kept at %s", stagingPath))
	}
	if plan.SafetyCopyEnabled {
		a.Runtime.Console(fmt.Sprintf("Safety mailbox tree kept at %s (delete manually after checking it)", plan.SafetyMailboxRoot))
	}
	a.Runtime.Console("Run `mail-backup backup` and then `mail-backup rebaseline` if this recovered state is now the intended truth.")
	return nil
}

func (a *RecoverApp) buildPlan(store recoverStore, snapshot kopiaSnapshot, stagingPath string, desiredMailboxes []recoverMailbox) (recoverPlan, error) {
	ignorePattern, err := regexp.Compile(a.Config.IgnoreMailboxRegex)
	if err != nil {
		return recoverPlan{}, fmt.Errorf("compile ignore_mailbox_regex: %w", err)
	}

	remoteMailboxes, err := store.ListManagedMailboxes(ignorePattern)
	if err != nil {
		return recoverPlan{}, err
	}

	desiredByName := map[string]recoverMailbox{}
	uploadCount := 0
	for _, mailbox := range desiredMailboxes {
		desiredByName[mailbox.Name] = mailbox
		uploadCount += mailbox.MessageCount
	}

	var clearMailboxes []recoverRemoteMailbox
	var deleteMailboxes []string
	deleteCount := 0

	remoteByName := map[string]recoverRemoteMailbox{}
	for _, mailbox := range remoteMailboxes {
		remoteByName[mailbox.Name] = mailbox
		clearMailboxes = append(clearMailboxes, mailbox)
		deleteCount += mailbox.MessageCount
		if _, ok := desiredByName[mailbox.Name]; !ok && mailbox.Name != "INBOX" && recoverMailboxDeletable(mailbox) {
			deleteMailboxes = append(deleteMailboxes, mailbox.Name)
		}
	}

	sort.Strings(deleteMailboxes)

	return recoverPlan{
		Snapshot:            snapshot,
		StagingPath:         stagingPath,
		SafetyMailboxRoot:   recoverySafetyMailboxRoot(recoverRunID(a.Runtime)),
		SafetyCopyEnabled:   a.SafetyCopy,
		DesiredMailboxes:    desiredMailboxes,
		ManagedRemote:       remoteMailboxes,
		CreateMailboxes:     missingMailboxNames(desiredMailboxes, remoteByName),
		ClearMailboxes:      clearMailboxes,
		DeleteMailboxes:     deleteMailboxes,
		UploadMessageCount:  uploadCount,
		DeleteMessageCount:  deleteCount,
		ManagedMailboxCount: len(desiredMailboxes),
	}, nil
}

func (a *RecoverApp) prepareRecoveryStaging(restoreApp *RestoreApp, snapshot kopiaSnapshot, stagingRoot string) (string, bool, error) {
	existing, err := findLatestRecoveryStaging(stagingRoot, snapshot.ShortID())
	if err != nil {
		return "", false, err
	}
	if existing != "" {
		reuse := a.YesFlag
		if !a.YesFlag {
			reuse, err = a.promptYesNo(fmt.Sprintf("Reuse existing staged snapshot at %s", existing), true)
			if err != nil {
				return "", false, err
			}
		}
		if reuse {
			return existing, false, nil
		}
	}

	stagingPath, err := os.MkdirTemp(stagingRoot, "snapshot-"+snapshot.ShortID()+"-")
	if err != nil {
		return "", false, fmt.Errorf("create recovery staging dir: %w", err)
	}
	if err := restoreApp.restoreSnapshot(snapshot, stagingPath); err != nil {
		return "", false, err
	}
	return stagingPath, true, nil
}

func (a *RecoverApp) printPlan(plan recoverPlan) {
	host, port, username, _ := a.imapConnectionInfo()

	a.Runtime.ConsoleRaw("\nRECOVERY PLAN\n")
	a.Runtime.ConsoleRaw(fmt.Sprintf("snapshot: %s (%s)\n", plan.Snapshot.ShortID(), plan.Snapshot.FormatTime()))
	a.Runtime.ConsoleRaw(fmt.Sprintf("imap: %s@%s:%s\n", username, host, port))
	a.Runtime.ConsoleRaw(fmt.Sprintf("staging: %s\n", plan.StagingPath))
	a.Runtime.ConsoleRaw(fmt.Sprintf("safety mailbox root: %s\n", plan.SafetyMailboxRoot))
	a.Runtime.ConsoleRaw(fmt.Sprintf("managed mailboxes in snapshot: %d\n", plan.ManagedMailboxCount))
	a.Runtime.ConsoleRaw(fmt.Sprintf("managed mailboxes currently on server: %d\n", len(plan.ManagedRemote)))
	a.Runtime.ConsoleRaw(fmt.Sprintf("mailboxes to create: %d\n", len(plan.CreateMailboxes)))
	a.Runtime.ConsoleRaw(fmt.Sprintf("remote messages to delete: %d\n", plan.DeleteMessageCount))
	a.Runtime.ConsoleRaw(fmt.Sprintf("remote mailboxes to delete: %d\n", len(plan.DeleteMailboxes)))
	a.Runtime.ConsoleRaw(fmt.Sprintf("messages to upload from snapshot: %d\n", plan.UploadMessageCount))
	if plan.SafetyCopyEnabled {
		a.Runtime.ConsoleRaw(fmt.Sprintf("safety copy: enabled, current managed remote mail will be copied under %s before recovery\n", plan.SafetyMailboxRoot))
		a.Runtime.ConsoleRaw("space warning: this may temporarily require about 2x space for the affected mailboxes on the server\n")
	} else {
		a.Runtime.ConsoleRaw("safety copy: disabled\n")
	}
	a.Runtime.ConsoleRaw(fmt.Sprintf("warning: mail that arrived after %s in managed mailboxes will be removed from those mailboxes\n", plan.Snapshot.FormatTime()))
	a.Runtime.ConsoleRaw("effect: the managed IMAP mailboxes will be rewritten to match the selected snapshot\n\n")
}

func (a *RecoverApp) confirm(plan recoverPlan) error {
	_, _, username, _ := a.imapConnectionInfo()
	phrase := username

	if a.YesFlag {
		if a.ConfirmUserFlag != phrase {
			return fmt.Errorf("recover requires -confirm-user %q when -yes is used", phrase)
		}
		return nil
	}

	ok, err := a.promptYesNo("Proceed with destructive IMAP recovery", false)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("recovery aborted")
	}

	a.Runtime.ConsoleRaw("This will delete mail from the server and re-upload the staged snapshot.\n")
	typed, err := a.prompt(fmt.Sprintf("Type the IMAP login user (%s) to continue: ", phrase))
	if err != nil {
		return err
	}
	if typed != phrase {
		return fmt.Errorf("confirmation did not match IMAP login user")
	}
	return nil
}

func (a *RecoverApp) executePlan(plan recoverPlan) error {
	if plan.SafetyCopyEnabled {
		if err := a.createSafetyMailboxTree(plan); err != nil {
			return err
		}
		if err := a.copyManagedMailToSafety(plan); err != nil {
			return err
		}
	}

	if len(plan.ClearMailboxes) > 0 {
		a.Runtime.Console("Clearing messages from managed remote mailboxes...")
		for _, mailbox := range sortRemoteMailboxesByDepth(plan.ClearMailboxes, true) {
			a.Runtime.LogFile("INFO", fmt.Sprintf("Clear remote mailbox: %s", mailbox.Name))
			var cleared int
			if err := a.withRecoverStoreRetry(fmt.Sprintf("clear remote mailbox %s", mailbox.Name), func(store recoverStore) error {
				var err error
				cleared, err = store.ClearMailbox(mailbox.Name)
				return err
			}); err != nil {
				return fmt.Errorf("clear remote mailbox %s: %w", mailbox.Name, err)
			}
			_ = cleared
		}
	}

	if len(plan.DeleteMailboxes) > 0 {
		a.Runtime.Console("Deleting empty remote mailboxes absent from the snapshot...")
		for _, mailbox := range sortMailboxNamesByDepth(plan.DeleteMailboxes, true) {
			a.Runtime.LogFile("INFO", fmt.Sprintf("Delete remote mailbox: %s", mailbox))
			if err := a.withRecoverStoreRetry(fmt.Sprintf("delete remote mailbox %s", mailbox), func(store recoverStore) error {
				return store.DeleteMailbox(mailbox)
			}); err != nil {
				return fmt.Errorf("delete remote mailbox %s: %w", mailbox, err)
			}
		}
	}

	a.Runtime.Console("Syncing staged snapshot to IMAP with mbsync...")
	recoveryConfigPath, err := a.writeRecoveryMbsyncConfig(plan.StagingPath)
	if err != nil {
		return err
	}
	command := a.recoveryMbsyncCommand(recoveryConfigPath)
	if _, err := a.Runtime.RunCommand(command, nil); err != nil {
		return fmt.Errorf("run recovery mbsync: %w", err)
	}
	return nil
}

func (a *RecoverApp) verifyPlan(plan recoverPlan) error {
	ignorePattern, err := regexp.Compile(a.Config.IgnoreMailboxRegex)
	if err != nil {
		return fmt.Errorf("compile ignore_mailbox_regex: %w", err)
	}

	a.Runtime.Console("Verifying managed IMAP mailbox counts...")
	var remoteMailboxes []recoverRemoteMailbox
	err = a.withRecoverStoreRetry("verify managed IMAP mailbox counts", func(store recoverStore) error {
		var listErr error
		remoteMailboxes, listErr = store.ListManagedMailboxes(ignorePattern)
		return listErr
	})
	if err != nil {
		return err
	}
	remoteByName := map[string]recoverRemoteMailbox{}
	for _, mailbox := range remoteMailboxes {
		remoteByName[mailbox.Name] = mailbox
	}

	for _, mailbox := range plan.DesiredMailboxes {
		remote, ok := remoteByName[mailbox.Name]
		if !ok {
			return fmt.Errorf("verification failed: mailbox missing on server: %s", mailbox.Name)
		}
		if remote.MessageCount != mailbox.MessageCount {
			return fmt.Errorf("verification failed: mailbox %s has %d messages on server, expected %d", mailbox.Name, remote.MessageCount, mailbox.MessageCount)
		}
	}

	desiredNames := map[string]struct{}{}
	for _, mailbox := range plan.DesiredMailboxes {
		desiredNames[mailbox.Name] = struct{}{}
	}
	for _, mailbox := range remoteMailboxes {
		if mailbox.Name == "INBOX" {
			if _, ok := desiredNames[mailbox.Name]; !ok && mailbox.MessageCount != 0 {
				return fmt.Errorf("verification failed: mailbox INBOX still has %d messages but the snapshot has none", mailbox.MessageCount)
			}
			continue
		}
		if _, ok := desiredNames[mailbox.Name]; !ok {
			return fmt.Errorf("verification failed: mailbox still exists on server but not in snapshot: %s", mailbox.Name)
		}
	}
	return nil
}

func (a *RecoverApp) imapConnectionInfo() (string, string, string, string) {
	host := strings.TrimSpace(a.Config.Env["IMAP_HOST"])
	port := strings.TrimSpace(a.Config.Env["IMAP_PORT"])
	username := strings.TrimSpace(a.Config.Env["IMAP_USERNAME"])
	password := strings.TrimSpace(a.Config.Env["IMAP_PASSWORD"])
	if port == "" {
		port = "993"
	}
	return host, port, username, password
}

func (a *RecoverApp) withRecoverStore(fn func(store recoverStore) error) error {
	store, err := connectRecoverStore(a.Config)
	if err != nil {
		return err
	}
	defer store.Close()
	return fn(store)
}

func (a *RecoverApp) withRecoverStoreRetry(action string, fn func(store recoverStore) error) error {
	err := a.withRecoverStore(fn)
	if err == nil || !isRecoverStoreRetryable(err) {
		return err
	}
	a.Runtime.Console(fmt.Sprintf("IMAP connection dropped while trying to %s. Reconnecting and retrying once...", action))
	a.Runtime.LogFile("WARN", fmt.Sprintf("Retrying after recoverable IMAP error during %s: %v", action, err))
	return a.withRecoverStore(fn)
}

func (a *RecoverApp) prompt(label string) (string, error) {
	if label != "" {
		fmt.Print(label)
	}
	line, err := a.stdin.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func (a *RecoverApp) promptYesNo(label string, defaultValue bool) (bool, error) {
	defaultText := "n"
	if defaultValue {
		defaultText = "y"
	}
	fmt.Printf("%s [%s]: ", label, defaultText)
	value, err := a.prompt("")
	if err != nil {
		return false, err
	}
	switch strings.ToLower(value) {
	case "":
		return defaultValue, nil
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid confirmation: %s", value)
	}
}

func collectRecoverMaildir(maildirRoot, ignoreMailboxRegex string) ([]recoverMailbox, error) {
	ignorePattern, err := regexp.Compile(ignoreMailboxRegex)
	if err != nil {
		return nil, fmt.Errorf("compile ignore_mailbox_regex: %w", err)
	}

	mailboxes := map[string]int{}
	err = filepath.WalkDir(maildirRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if path != maildirRoot && looksLikeMailboxDir(path) {
				mailbox := mailboxName(maildirRoot, filepath.Join(path, "cur", "placeholder"))
				if ignorePattern.String() == "" || !ignorePattern.MatchString(mailbox) {
					if _, ok := mailboxes[mailbox]; !ok {
						mailboxes[mailbox] = 0
					}
				}
			}
			if d.Name() == "tmp" {
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
			return nil
		}

		mailboxes[mailbox]++
		return nil
	})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(mailboxes))
	for name := range mailboxes {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make([]recoverMailbox, 0, len(names))
	for _, name := range names {
		result = append(result, recoverMailbox{Name: name, MessageCount: mailboxes[name]})
	}
	return result, nil
}

func findLatestRecoveryStaging(stagingRoot, snapshotShortID string) (string, error) {
	entries, err := os.ReadDir(stagingRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read recovery staging dir: %w", err)
	}

	prefix := "snapshot-" + snapshotShortID + "-"
	var newestPath string
	var newestMod time.Time
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		fullPath := filepath.Join(stagingRoot, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return "", fmt.Errorf("stat recovery staging dir %s: %w", fullPath, err)
		}
		if newestPath == "" || info.ModTime().After(newestMod) {
			newestPath = fullPath
			newestMod = info.ModTime()
		}
	}
	return newestPath, nil
}

func normalizeRecoveryMaildirTimes(maildirRoot, ignoreMailboxRegex string) error {
	ignorePattern, err := regexp.Compile(ignoreMailboxRegex)
	if err != nil {
		return fmt.Errorf("compile ignore_mailbox_regex: %w", err)
	}

	err = filepath.WalkDir(maildirRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == "tmp" {
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
			return nil
		}

		rawMessage, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read recovered message %s: %w", relativePath(maildirRoot, path), err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat recovered message %s: %w", relativePath(maildirRoot, path), err)
		}
		msg, err := mail.ReadMessage(bytes.NewReader(rawMessage))
		if err != nil {
			return nil
		}
		messageTS := parseMessageTimestamp(msg.Header.Get("Date"), info.ModTime())
		messageTime := time.Unix(int64(messageTS), 0)
		return os.Chtimes(path, messageTime, messageTime)
	})
	if err != nil {
		return err
	}
	return nil
}

type imapRecoverStore struct {
	client *client.Client
}

func connectRecoverStore(config Config) (recoverStore, error) {
	host := strings.TrimSpace(config.Env["IMAP_HOST"])
	port := strings.TrimSpace(config.Env["IMAP_PORT"])
	username := strings.TrimSpace(config.Env["IMAP_USERNAME"])
	password := strings.TrimSpace(config.Env["IMAP_PASSWORD"])

	if host == "" {
		return nil, fmt.Errorf("IMAP_HOST must be set in .env")
	}
	if username == "" {
		return nil, fmt.Errorf("IMAP_USERNAME must be set in .env")
	}
	if password == "" {
		return nil, fmt.Errorf("IMAP_PASSWORD must be set in .env")
	}
	if port == "" {
		port = "993"
	}

	address := host + ":" + port
	conn, err := client.DialTLS(address, nil)
	if err != nil {
		return nil, fmt.Errorf("connect to IMAP %s: %w", address, err)
	}
	if err := conn.Login(username, password); err != nil {
		_ = conn.Logout()
		return nil, fmt.Errorf("login to IMAP %s as %s: %w", address, username, err)
	}
	return &imapRecoverStore{client: conn}, nil
}

func (s *imapRecoverStore) ListManagedMailboxes(ignorePattern *regexp.Regexp) ([]recoverRemoteMailbox, error) {
	return s.listMailboxes(func(name string) bool {
		return recoverMailboxIgnored(name, ignorePattern)
	})
}

func (s *imapRecoverStore) listMailboxes(ignore func(name string) bool) ([]recoverRemoteMailbox, error) {
	mailboxes := make(chan *imap.MailboxInfo, 32)
	done := make(chan error, 1)
	go func() {
		done <- s.client.List("", "*", mailboxes)
	}()

	var result []recoverRemoteMailbox
	for mailbox := range mailboxes {
		if contains(mailbox.Attributes, imap.NoSelectAttr) {
			continue
		}
		if ignore(mailbox.Name) {
			continue
		}
		count, err := s.mailboxMessageCount(mailbox.Name)
		if err != nil {
			return nil, err
		}
		result = append(result, recoverRemoteMailbox{Name: mailbox.Name, MessageCount: count, Attributes: append([]string(nil), mailbox.Attributes...)})
	}
	if err := <-done; err != nil {
		return nil, fmt.Errorf("list IMAP mailboxes: %w", err)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func (s *imapRecoverStore) mailboxMessageCount(name string) (int, error) {
	if _, err := s.client.Select(name, false); err != nil {
		return 0, fmt.Errorf("select IMAP mailbox %s: %w", name, err)
	}
	criteria := imap.NewSearchCriteria()
	ids, err := s.client.Search(criteria)
	if err != nil {
		return 0, fmt.Errorf("search IMAP mailbox %s: %w", name, err)
	}
	return len(ids), nil
}

func (s *imapRecoverStore) CreateMailbox(name string) error {
	if err := s.client.Create(name); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return err
	}
	return nil
}

func (s *imapRecoverStore) CopyMailboxMessages(sourceMailbox, targetMailbox string) (int, error) {
	if _, err := s.client.Select(sourceMailbox, false); err != nil {
		return 0, fmt.Errorf("select IMAP mailbox %s: %w", sourceMailbox, err)
	}
	criteria := imap.NewSearchCriteria()
	ids, err := s.client.Search(criteria)
	if err != nil {
		return 0, fmt.Errorf("search IMAP mailbox %s: %w", sourceMailbox, err)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	seqset := new(imap.SeqSet)
	seqset.AddNum(ids...)
	if err := s.client.Copy(seqset, targetMailbox); err != nil {
		return 0, fmt.Errorf("copy IMAP mailbox %s to %s: %w", sourceMailbox, targetMailbox, err)
	}
	return len(ids), nil
}

func (s *imapRecoverStore) ClearMailbox(name string) (int, error) {
	if _, err := s.client.Select(name, false); err != nil {
		return 0, fmt.Errorf("select IMAP mailbox %s: %w", name, err)
	}
	criteria := imap.NewSearchCriteria()
	ids, err := s.client.Search(criteria)
	if err != nil {
		return 0, fmt.Errorf("search IMAP mailbox %s: %w", name, err)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	seqset := new(imap.SeqSet)
	seqset.AddNum(ids...)
	item := imap.FormatFlagsOp(imap.AddFlags, true)
	flags := []interface{}{imap.DeletedFlag}
	if err := s.client.Store(seqset, item, flags, nil); err != nil {
		return 0, fmt.Errorf("mark messages deleted in %s: %w", name, err)
	}
	if err := s.client.Expunge(nil); err != nil {
		return 0, fmt.Errorf("expunge mailbox %s: %w", name, err)
	}
	return len(ids), nil
}

func (s *imapRecoverStore) DeleteMailbox(name string) error {
	return s.client.Delete(name)
}

func (s *imapRecoverStore) Close() error {
	return s.client.Logout()
}

func sortMailboxNamesByDepth(names []string, reverse bool) []string {
	sorted := append([]string(nil), names...)
	sort.Slice(sorted, func(i, j int) bool {
		leftDepth := strings.Count(sorted[i], "/")
		rightDepth := strings.Count(sorted[j], "/")
		if leftDepth != rightDepth {
			if reverse {
				return leftDepth > rightDepth
			}
			return leftDepth < rightDepth
		}
		return sorted[i] < sorted[j]
	})
	return sorted
}

func sortRemoteMailboxesByDepth(mailboxes []recoverRemoteMailbox, reverse bool) []recoverRemoteMailbox {
	sorted := append([]recoverRemoteMailbox(nil), mailboxes...)
	sort.Slice(sorted, func(i, j int) bool {
		leftDepth := strings.Count(sorted[i].Name, "/")
		rightDepth := strings.Count(sorted[j].Name, "/")
		if leftDepth != rightDepth {
			if reverse {
				return leftDepth > rightDepth
			}
			return leftDepth < rightDepth
		}
		return sorted[i].Name < sorted[j].Name
	})
	return sorted
}

func looksLikeMailboxDir(path string) bool {
	for _, child := range []string{"cur", "new", "tmp"} {
		info, err := os.Stat(filepath.Join(path, child))
		if err != nil || !info.IsDir() {
			return false
		}
	}
	return true
}

func recoverMailboxDeletable(mailbox recoverRemoteMailbox) bool {
	for _, attr := range mailbox.Attributes {
		switch attr {
		case imap.ArchiveAttr, imap.DraftsAttr, imap.SentAttr, imap.TrashAttr, imap.AllAttr, imap.JunkAttr:
			return false
		}
	}
	return true
}

func recoverMailboxIgnored(name string, ignorePattern *regexp.Regexp) bool {
	if strings.HasPrefix(name, "Recovery-Safety-") {
		return true
	}
	return ignorePattern != nil && ignorePattern.String() != "" && ignorePattern.MatchString(name)
}

func isRecoverStoreRetryable(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	for _, needle := range []string{
		"broken pipe",
		"connection reset by peer",
		"connection closed",
		"use of closed network connection",
		"unexpected eof",
		"eof",
		"i/o timeout",
		"read: timeout",
		"write: timeout",
	} {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func (a *RecoverApp) chooseSafetyCopy(plan *recoverPlan) error {
	safetyCopy, err := a.promptYesNo("Create a server-side safety copy before recovery (recommended, uses about 2x temporary mailbox space)", true)
	if err != nil {
		return err
	}
	a.SafetyCopy = safetyCopy
	plan.SafetyCopyEnabled = safetyCopy
	return nil
}

func recoverySafetyMailboxRoot(runID string) string {
	stamp := strings.TrimSuffix(runID, "Z")
	if stamp == "" {
		stamp = "manual"
	}
	return "Recovery-Safety-" + stamp
}

func recoverySafetyMailboxName(root, mailbox string) string {
	if mailbox == "INBOX" {
		return root + "/INBOX"
	}
	return root + "/" + mailbox
}

func recoverRunID(runtime *Runtime) string {
	if runtime == nil {
		return ""
	}
	return runtime.RunID
}

func (a *RecoverApp) createSafetyMailboxTree(plan recoverPlan) error {
	a.Runtime.Console("Creating recovery safety mailbox tree...")
	rootAndChildren := []string{plan.SafetyMailboxRoot}
	for _, mailbox := range plan.ManagedRemote {
		rootAndChildren = append(rootAndChildren, recoverySafetyMailboxName(plan.SafetyMailboxRoot, mailbox.Name))
	}
	for _, mailbox := range sortMailboxNamesByDepth(rootAndChildren, false) {
		a.Runtime.LogFile("INFO", fmt.Sprintf("Create safety mailbox: %s", mailbox))
		if err := a.withRecoverStoreRetry(fmt.Sprintf("create safety mailbox %s", mailbox), func(store recoverStore) error {
			return store.CreateMailbox(mailbox)
		}); err != nil {
			return fmt.Errorf("create safety mailbox %s: %w", mailbox, err)
		}
	}
	return nil
}

func (a *RecoverApp) copyManagedMailToSafety(plan recoverPlan) error {
	a.Runtime.Console("Copying current managed remote mail into the safety mailbox tree...")
	for _, mailbox := range sortRemoteMailboxesByDepth(plan.ManagedRemote, false) {
		target := recoverySafetyMailboxName(plan.SafetyMailboxRoot, mailbox.Name)
		a.Runtime.LogFile("INFO", fmt.Sprintf("Copy remote mailbox %s to safety mailbox %s", mailbox.Name, target))
		if err := a.withRecoverStoreRetry(fmt.Sprintf("copy remote mailbox %s to safety mailbox %s", mailbox.Name, target), func(store recoverStore) error {
			_, err := store.CopyMailboxMessages(mailbox.Name, target)
			return err
		}); err != nil {
			return fmt.Errorf("copy remote mailbox %s to safety mailbox %s: %w", mailbox.Name, target, err)
		}
	}
	return nil
}

func (a *RecoverApp) postRecoveryCleanup(plan recoverPlan) error {
	if a.YesFlag {
		return nil
	}

	deleteStaging, err := a.promptYesNo("Delete the local staged restore directory now", false)
	if err != nil {
		return err
	}
	if deleteStaging {
		if err := os.RemoveAll(plan.StagingPath); err != nil {
			return fmt.Errorf("delete staged restore directory %s: %w", plan.StagingPath, err)
		}
		a.Runtime.Console("Deleted local staged restore directory.")
	}
	return nil
}

func missingMailboxNames(desired []recoverMailbox, remoteByName map[string]recoverRemoteMailbox) []string {
	var names []string
	for _, mailbox := range desired {
		if mailbox.Name == "INBOX" {
			continue
		}
		if _, ok := remoteByName[mailbox.Name]; !ok {
			names = append(names, mailbox.Name)
		}
	}
	sort.Strings(names)
	return names
}

func (a *RecoverApp) writeRecoveryMbsyncConfig(stagingPath string) (string, error) {
	configPath := filepath.Join(a.Config.StateDir, fmt.Sprintf("mbsyncrc.recover.%s", a.Runtime.RunID))
	syncStateDir := filepath.Join(a.Config.StateDir, "recover-syncstate", a.Runtime.RunID)
	if err := os.MkdirAll(syncStateDir, 0o755); err != nil {
		return "", fmt.Errorf("create recovery sync state dir: %w", err)
	}
	content := renderRecoveryMbsyncConfig(
		strings.TrimSpace(a.Config.Env["IMAP_HOST"]),
		strings.TrimSpace(a.Config.Env["IMAP_PORT"]),
		strings.TrimSpace(a.Config.Env["IMAP_USERNAME"]),
		stagingPath,
		syncStateDir,
		recoveryMbsyncPatterns(a.Config.IgnoreMailboxRegex),
	)
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		return "", fmt.Errorf("write recovery mbsync config: %w", err)
	}
	return configPath, nil
}

func (a *RecoverApp) recoveryMbsyncCommand(configPath string) []string {
	binary := "mbsync"
	if len(a.Config.MbsyncCommand) > 0 {
		binary = a.Config.MbsyncCommand[0]
	}
	return []string{binary, "-c", configPath, "mail-backup-recover"}
}

func renderRecoveryMbsyncConfig(host, port, username, maildirPath, syncStateDir string, patterns []string) string {
	if port == "" {
		port = "993"
	}
	if len(patterns) == 0 {
		patterns = []string{"INBOX", "*", "!Recovery-Safety-*"}
	}
	return strings.Join([]string{
		"IMAPAccount remote",
		fmt.Sprintf("Host %s", host),
		fmt.Sprintf("Port %s", port),
		"TLSType IMAPS",
		"AuthMechs LOGIN",
		fmt.Sprintf("User %s", username),
		`PassCmd "/bin/sh -c 'printf %s \"$IMAP_PASSWORD\"'"`,
		"",
		"IMAPStore remote-store",
		"Account remote",
		"",
		"MaildirStore local-store",
		fmt.Sprintf("Path %q", filepath.ToSlash(filepath.Clean(maildirPath))+"/"),
		fmt.Sprintf("Inbox %q", filepath.ToSlash(filepath.Join(filepath.Clean(maildirPath), "INBOX"))),
		"SubFolders Verbatim",
		"",
		"Channel mail-backup-recover",
		"Far :remote-store:",
		"Near :local-store:",
		"Patterns " + strings.Join(patterns, " "),
		"Create Far",
		"Sync Push",
		"CopyArrivalDate yes",
		fmt.Sprintf("SyncState %q", filepath.ToSlash(filepath.Clean(syncStateDir))+"/"),
		"",
	}, "\n")
}

func recoveryMbsyncPatterns(ignoreMailboxRegex string) []string {
	patterns := []string{"INBOX", "*", "!Recovery-Safety-*"}
	if ignoreMailboxRegex == defaultIgnoreMailboxRegex {
		return append(patterns,
			"!Trash", "!Trash/*",
			"!Junk", "!Junk/*",
			"!Spam", "!Spam/*",
			"!Drafts", "!Drafts/*",
		)
	}
	return patterns
}
