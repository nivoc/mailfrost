package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type StatePaths struct {
	StateDir              string
	LogDir                string
	ManifestDir           string
	ReportDir             string
	StatusFile            string
	AlertLog              string
	KopiaMaintenanceStamp string
	LockFile              string
}

func StatePathsFromDir(stateDir string) StatePaths {
	return StatePaths{
		StateDir:              stateDir,
		LogDir:                filepath.Join(stateDir, "logs"),
		ManifestDir:           filepath.Join(stateDir, "manifests"),
		ReportDir:             filepath.Join(stateDir, "reports"),
		StatusFile:            filepath.Join(stateDir, "status.json"),
		AlertLog:              filepath.Join(stateDir, "alerts.log"),
		KopiaMaintenanceStamp: filepath.Join(stateDir, "kopia-maintenance.last_ok"),
		LockFile:              filepath.Join(stateDir, ".lock"),
	}
}

func (p StatePaths) Create() error {
	for _, dir := range []string{p.LogDir, p.ManifestDir, p.ReportDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

type Runtime struct {
	Config      Config
	Paths       StatePaths
	RunID       string
	CommandName string
	RunLogPath  string
	runLog      *os.File
	lockFile    *os.File
	finalized   bool
}

func StartRuntime(config Config, commandName string) (*Runtime, error) {
	paths := StatePathsFromDir(config.StateDir)
	if err := paths.Create(); err != nil {
		return nil, fmt.Errorf("create state directories: %w", err)
	}

	lockFile, err := os.OpenFile(paths.LockFile, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("another backup run appears to be active: %s", paths.LockFile)
	}

	runID := time.Now().UTC().Format("20060102T150405Z")
	if err := lockFile.Truncate(0); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("truncate lock file: %w", err)
	}
	if _, err := lockFile.Seek(0, 0); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("seek lock file: %w", err)
	}
	payload, _ := json.Marshal(map[string]any{"pid": os.Getpid(), "started_at": runID})
	if _, err := lockFile.Write(append(payload, '\n')); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("write lock file: %w", err)
	}

	runLogPath := filepath.Join(paths.LogDir, fmt.Sprintf("run-%s.log", runID))
	runLog, err := os.OpenFile(runLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("open run log: %w", err)
	}

	runtime := &Runtime{
		Config:      config,
		Paths:       paths,
		RunID:       runID,
		CommandName: commandName,
		RunLogPath:  runLogPath,
		runLog:      runLog,
		lockFile:    lockFile,
	}
	runtime.LogFile("INFO", fmt.Sprintf("Run id: %s", runtime.RunID))
	if runtime.Config.AdvancedConfigFileLoaded {
		runtime.LogFile("INFO", fmt.Sprintf("Advanced config file: %s", runtime.Config.AdvancedConfigPath))
	}
	if runtime.Config.ConfigFileLoaded {
		runtime.LogFile("INFO", fmt.Sprintf("Config file: %s", runtime.Config.ConfigPath))
	} else if !runtime.Config.AdvancedConfigFileLoaded {
		runtime.LogFile("INFO", fmt.Sprintf("Config file: %s (no config files loaded, using defaults)", runtime.Config.ConfigPath))
	}
	runtime.LogFile("INFO", fmt.Sprintf("Env file: %s", runtime.Config.EnvPath))
	runtime.LogFile("INFO", fmt.Sprintf("State dir: %s", runtime.Config.StateDir))
	if err := RegisterInstance(config); err != nil {
		runtime.LogFile("WARN", fmt.Sprintf("Register instance failed: %s", err))
	}
	if err := runtime.writeRunningStatus(); err != nil {
		runtime.LogFile("WARN", fmt.Sprintf("Write status file failed: %s", err))
	}
	return runtime, nil
}

func (r *Runtime) Close() {
	if r.lockFile != nil {
		_ = syscall.Flock(int(r.lockFile.Fd()), syscall.LOCK_UN)
		_ = r.lockFile.Close()
	}
	if r.runLog != nil {
		_ = r.runLog.Close()
	}
}

func (r *Runtime) LogFile(level, message string) {
	line := fmt.Sprintf("%s [%s] %s", timestampLocal(), level, message)
	_, _ = r.runLog.WriteString(line + "\n")
}

func (r *Runtime) LogFileRaw(text string) {
	_, _ = r.runLog.WriteString(text)
}

func (r *Runtime) Console(message string) {
	fmt.Printf("%s %s\n", consoleTimestamp(), message)
}

func (r *Runtime) ConsoleRaw(text string) {
	fmt.Print(text)
}

func (r *Runtime) CommandEnvMap(extra map[string]string) map[string]string {
	envMap := map[string]string{}
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			envMap[key] = value
		}
	}
	for key, value := range r.Config.Env {
		envMap[key] = value
	}
	for key, value := range extra {
		envMap[key] = value
	}
	return envMap
}

func (r *Runtime) CommandEnv(extra map[string]string) []string {
	envMap := r.CommandEnvMap(extra)
	env := make([]string, 0, len(envMap))
	for key, value := range envMap {
		env = append(env, key+"="+value)
	}
	sort.Strings(env)
	return env
}

func (r *Runtime) RunCommand(command []string, extraEnv map[string]string, stdinData ...string) (string, error) {
	displayCommand := sanitizeCommandForDisplay(command)
	command = wrapCommandForTTY(command)
	r.LogFile("INFO", fmt.Sprintf("Running command: %s", strings.Join(displayCommand, " ")))

	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = r.CommandEnv(extraEnv)
	cmd.Dir = filepath.Dir(r.Config.ConfigPath)
	if len(stdinData) > 0 && stdinData[0] != "" {
		cmd.Stdin = strings.NewReader(stdinData[0])
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("capture command stdout %s: %w", strings.Join(command, " "), err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("capture command stderr %s: %w", strings.Join(command, " "), err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start command %s: %w", strings.Join(command, " "), err)
	}

	var (
		output bytes.Buffer
		mu     sync.Mutex
		wg     sync.WaitGroup
	)
	statusWidth := 0
	statusOpen := false
	consoleFilter := newConsoleMirrorFilter(command)
	mirrorToConsole := consoleFilter != nil
	copyOutput := func(pipe io.ReadCloser) {
		defer wg.Done()
		buffer := make([]byte, 32*1024)
		var pending string
		for {
			n, readErr := pipe.Read(buffer)
			if n > 0 {
				chunk := string(buffer[:n])
				mu.Lock()
				_, _ = output.Write(buffer[:n])
				mu.Unlock()
				r.LogFileRaw(chunk)
				if mirrorToConsole {
					pending = consoleFilter.consume(r, pending, chunk)
				}
			}
			if readErr == nil {
				continue
			}
			if mirrorToConsole {
				consoleFilter.finish(r, pending)
			}
			if readErr == io.EOF {
				return
			}
			r.LogFile("WARN", fmt.Sprintf("Read command output failed for %s: %v", strings.Join(displayCommand, " "), readErr))
			return
		}
	}
	wg.Add(2)
	go copyOutput(stdoutPipe)
	go copyOutput(stderrPipe)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	startedAt := time.Now()
	if !mirrorToConsole {
		r.Console(fmt.Sprintf("Running command: %s", strings.Join(displayCommand, " ")))
	}
	for {
		select {
		case err = <-done:
			wg.Wait()
			goto finished
		case <-ticker.C:
			if mirrorToConsole {
				continue
			}
			elapsed := time.Since(startedAt).Round(time.Second)
			message := fmt.Sprintf("Still running: %s (%s elapsed)", strings.Join(displayCommand, " "), elapsed)
			rendered := fmt.Sprintf("%s Still running... (%s elapsed)", consoleTimestamp(), elapsed)
			r.ConsoleRaw(renderTransientStatus(rendered, statusWidth))
			statusWidth = max(statusWidth, len(rendered))
			statusOpen = true
			r.LogFile("INFO", message)
		}
	}

finished:
	if statusOpen {
		r.ConsoleRaw("\n")
	}
	outputText := output.String()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return outputText, fmt.Errorf("command failed with exit code %d: %s", exitErr.ExitCode(), strings.Join(displayCommand, " "))
		}
		return outputText, fmt.Errorf("run command %s: %w", strings.Join(displayCommand, " "), err)
	}
	return outputText, nil
}

func renderTransientStatus(rendered string, priorWidth int) string {
	padding := ""
	if priorWidth > len(rendered) {
		padding = strings.Repeat(" ", priorWidth-len(rendered))
	}
	return "\r" + rendered + padding
}

func sanitizeCommandForDisplay(command []string) []string {
	sanitized := append([]string(nil), command...)
	for i := 0; i < len(sanitized); i++ {
		part := sanitized[i]
		switch {
		case part == "--password":
			if i+1 < len(sanitized) {
				sanitized[i+1] = "<redacted>"
				i++
			}
		case strings.HasPrefix(part, "--password="):
			sanitized[i] = "--password=<redacted>"
		}
	}
	return sanitized
}

type consoleMirrorFilter interface {
	consume(runtime *Runtime, pending, chunk string) string
	finish(runtime *Runtime, pending string)
}

func newConsoleMirrorFilter(command []string) consoleMirrorFilter {
	if filter := newMbsyncConsoleFilter(command); filter != nil {
		return filter
	}
	if filter := newKopiaSnapshotConsoleFilter(command); filter != nil {
		return filter
	}
	return nil
}

func wrapCommandForTTY(command []string) []string {
	if len(command) == 0 {
		return command
	}
	if filepath.Base(command[0]) != "mbsync" {
		return command
	}
	if _, err := os.Stat("/usr/bin/script"); err != nil {
		return command
	}
	return append([]string{"/usr/bin/script", "-q", "/dev/null"}, command...)
}

type mbsyncConsoleFilter struct {
	mode          string
	lastFormatted string
	progressWidth int
	progressOpen  bool
}

func newMbsyncConsoleFilter(command []string) *mbsyncConsoleFilter {
	if len(command) == 0 {
		return nil
	}
	base := filepath.Base(command[0])
	if base == "mbsync" {
		return &mbsyncConsoleFilter{mode: detectMbsyncMode(command)}
	}
	if base == "script" && len(command) >= 4 && filepath.Base(command[3]) == "mbsync" {
		return &mbsyncConsoleFilter{mode: detectMbsyncMode(command[3:])}
	}
	return nil
}

func (f *mbsyncConsoleFilter) consume(runtime *Runtime, pending, chunk string) string {
	combined := pending + chunk
	for {
		split := strings.IndexAny(combined, "\r\n")
		if split < 0 {
			return combined
		}
		line := combined[:split]
		if strings.TrimSpace(line) != "" {
			f.emit(runtime, line)
		}
		combined = combined[split+1:]
	}
}

func (f *mbsyncConsoleFilter) finish(runtime *Runtime, pending string) {
	if strings.TrimSpace(pending) != "" {
		f.emit(runtime, pending)
	}
	f.finishProgress(runtime)
}

func (f *mbsyncConsoleFilter) emit(runtime *Runtime, line string) {
	if formatted, ok := formatMbsyncProgressLineForMode(line, f.mode); ok {
		if formatted == f.lastFormatted {
			return
		}
		f.lastFormatted = formatted
		if strings.HasPrefix(formatted, "Summary |") {
			f.finishProgress(runtime)
			runtime.Console(formatted)
			return
		}
		f.renderProgress(runtime, formatted)
		return
	}
	f.lastFormatted = ""
	f.finishProgress(runtime)
	f.emitRaw(runtime, line)
}

func (f *mbsyncConsoleFilter) emitRaw(runtime *Runtime, line string) {
	trimmed := strings.TrimSpace(stripANSI(line))
	if trimmed == "" {
		return
	}
	runtime.ConsoleRaw(trimmed + "\n")
}

func (f *mbsyncConsoleFilter) renderProgress(runtime *Runtime, formatted string) {
	rendered := fmt.Sprintf("%s %s", consoleTimestamp(), formatted)
	padding := ""
	if f.progressWidth > len(rendered) {
		padding = strings.Repeat(" ", f.progressWidth-len(rendered))
	}
	runtime.ConsoleRaw("\r" + rendered + padding)
	f.progressWidth = max(f.progressWidth, len(rendered))
	f.progressOpen = true
}

func (f *mbsyncConsoleFilter) finishProgress(runtime *Runtime) {
	if !f.progressOpen {
		return
	}
	runtime.ConsoleRaw("\n")
	f.progressOpen = false
	f.progressWidth = 0
}

type kopiaSnapshotConsoleFilter struct {
	progressWidth int
	progressOpen  bool
}

func newKopiaSnapshotConsoleFilter(command []string) *kopiaSnapshotConsoleFilter {
	if len(command) < 3 {
		return nil
	}
	if filepath.Base(command[0]) != "kopia" {
		return nil
	}
	if command[1] != "snapshot" || command[2] != "create" {
		return nil
	}
	return &kopiaSnapshotConsoleFilter{}
}

func (f *kopiaSnapshotConsoleFilter) consume(runtime *Runtime, pending, chunk string) string {
	combined := pending + chunk
	for {
		split := strings.IndexAny(combined, "\r\n")
		if split < 0 {
			return combined
		}
		line := combined[:split]
		if strings.TrimSpace(line) != "" {
			f.emit(runtime, line)
		}
		combined = combined[split+1:]
	}
}

func (f *kopiaSnapshotConsoleFilter) finish(runtime *Runtime, pending string) {
	if strings.TrimSpace(pending) != "" {
		f.emit(runtime, pending)
	}
	f.finishProgress(runtime)
}

func (f *kopiaSnapshotConsoleFilter) emit(runtime *Runtime, line string) {
	trimmed := strings.TrimSpace(stripANSI(line))
	if trimmed == "" {
		return
	}
	if strings.HasPrefix(trimmed, "{") {
		return
	}
	if strings.HasPrefix(trimmed, "Snapshotting ") || strings.Contains(trimmed, " hashing,") {
		f.renderProgress(runtime, trimmed)
		return
	}
	f.finishProgress(runtime)
	runtime.ConsoleRaw(trimmed + "\n")
}

func (f *kopiaSnapshotConsoleFilter) renderProgress(runtime *Runtime, line string) {
	padding := ""
	if f.progressWidth > len(line) {
		padding = strings.Repeat(" ", f.progressWidth-len(line))
	}
	runtime.ConsoleRaw("\r" + line + padding)
	f.progressWidth = max(f.progressWidth, len(line))
	f.progressOpen = true
}

func (f *kopiaSnapshotConsoleFilter) finishProgress(runtime *Runtime) {
	if !f.progressOpen {
		return
	}
	runtime.ConsoleRaw("\n")
	f.progressOpen = false
	f.progressWidth = 0
}

var (
	ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)
)

func stripANSI(value string) string {
	value = ansiEscapePattern.ReplaceAllString(value, "")
	runes := make([]rune, 0, len(value))
	for _, r := range value {
		switch r {
		case '\b', 0x7f:
			if len(runes) > 0 {
				runes = runes[:len(runes)-1]
			}
		default:
			if r < 0x20 {
				continue
			}
			runes = append(runes, r)
		}
	}
	return string(runes)
}

func formatMbsyncProgressLineForMode(line, mode string) (string, bool) {
	line = strings.TrimSpace(stripANSI(line))
	fields := strings.Fields(line)
	if len(fields) >= 14 && fields[0] == "C:" && fields[2] == "B:" && fields[4] == "F:" && fields[9] == "N:" {
		mailboxProgress := fields[3]
		farAdd := strings.TrimPrefix(fields[5], "+")
		farDelete := strings.TrimPrefix(fields[8], "-")
		nearAdd := strings.TrimPrefix(fields[10], "+")
		if mode == "recover" {
			return fmt.Sprintf("Mailbox %s | IMAP upload %s | delete %s", mailboxProgress, farAdd, farDelete), true
		}
		return fmt.Sprintf("Mailbox %s | remote %s | local %s", mailboxProgress, farAdd, nearAdd), true
	}
	if len(fields) >= 14 && fields[0] == "Channels:" && fields[2] == "Boxes:" && fields[4] == "Far:" && fields[9] == "Near:" {
		boxes := fields[3]
		farAdd := strings.TrimPrefix(fields[5], "+")
		farDelete := strings.TrimPrefix(fields[8], "-")
		nearAdd := strings.TrimPrefix(fields[10], "+")
		if mode == "recover" {
			return fmt.Sprintf("Summary | Mailboxes %s | IMAP upload %s | delete %s", boxes, farAdd, farDelete), true
		}
		return fmt.Sprintf("Summary | Mailboxes %s | remote %s | local %s", boxes, farAdd, nearAdd), true
	}
	return "", false
}

func detectMbsyncMode(command []string) string {
	for _, part := range command {
		if part == defaultRecoverChannelName {
			return "recover"
		}
	}
	return "backup"
}

func (r *Runtime) SendAlert(status, subject, body string) {
	alertLine := fmt.Sprintf("%s [%s] %s", timestampLocal(), status, subject)
	file, err := os.OpenFile(r.Paths.AlertLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err == nil {
		_, _ = file.WriteString(alertLine + "\n")
		_ = file.Close()
	}

	if strings.TrimSpace(r.Config.AlertCommand) == "" {
		return
	}

	cmd := exec.Command("/bin/bash", "-c", r.Config.AlertCommand)
	cmd.Env = r.CommandEnv(map[string]string{
		"MAIL_BACKUP_STATUS":        status,
		"MAIL_BACKUP_ALERT_SUBJECT": subject,
		"MAIL_BACKUP_ALERT_BODY":    body,
	})
	cmd.Dir = filepath.Dir(r.Config.ConfigPath)
	cmd.Stdin = strings.NewReader(body)
	output, err := cmd.CombinedOutput()
	if err != nil {
		r.LogFile("WARN", fmt.Sprintf("Alert command failed: %s", r.Config.AlertCommand))
	}
	if len(output) > 0 {
		r.LogFileRaw(string(output))
	}
}

func NotifyRuntimeFailure(runtime *Runtime, err error) {
	runtime.LogFile("ERROR", err.Error())
	runtime.Console(fmt.Sprintf("ERROR: %s", err.Error()))
	runtime.ConsoleRaw(fmt.Sprintf("         See full log: %s\n", runtime.RunLogPath))
	body := strings.Join([]string{
		"Status: ERROR",
		fmt.Sprintf("Error: %s", err.Error()),
		fmt.Sprintf("Run log: %s", runtime.RunLogPath),
	}, "\n")
	runtime.SendAlert("ERROR", "Mail backup run failed", body)
}

func (r *Runtime) MarkCompleted(status string, exitCode int, runErr error) {
	if r == nil || r.finalized {
		return
	}
	if err := r.writeCompletedStatus(status, exitCode, runErr); err != nil {
		r.LogFile("WARN", fmt.Sprintf("Write status file failed: %s", err))
	}
	r.finalized = true
}

func (r *Runtime) writeRunningStatus() error {
	snapshot, err := loadInstanceStatus(r.Paths.StatusFile)
	if err != nil {
		return err
	}
	snapshot.UpdatedAt = utcNow().Format(time.RFC3339)
	snapshot.Current = &RunStatus{
		RunID:     r.RunID,
		Command:   r.CommandName,
		Status:    "running",
		PID:       os.Getpid(),
		StartedAt: utcNow().Format(time.RFC3339),
		LogPath:   r.RunLogPath,
	}
	return saveInstanceStatus(r.Paths.StatusFile, snapshot)
}

func (r *Runtime) writeCompletedStatus(status string, exitCode int, runErr error) error {
	snapshot, err := loadInstanceStatus(r.Paths.StatusFile)
	if err != nil {
		return err
	}
	startedAt := utcNow().Format(time.RFC3339)
	if snapshot.Current != nil && snapshot.Current.RunID == r.RunID && strings.TrimSpace(snapshot.Current.StartedAt) != "" {
		startedAt = snapshot.Current.StartedAt
	}

	runStatus := RunStatus{
		RunID:      r.RunID,
		Command:    r.CommandName,
		Status:     strings.TrimSpace(status),
		PID:        os.Getpid(),
		StartedAt:  startedAt,
		FinishedAt: utcNow().Format(time.RFC3339),
		ExitCode:   exitCode,
		LogPath:    r.RunLogPath,
	}
	if runErr != nil {
		runStatus.Error = runErr.Error()
	}

	snapshot.UpdatedAt = utcNow().Format(time.RFC3339)
	snapshot.Current = nil
	snapshot.Last = &runStatus
	return saveInstanceStatus(r.Paths.StatusFile, snapshot)
}

func timestampLocal() string {
	return time.Now().Format("2006-01-02T15:04:05-0700")
}

func consoleTimestamp() string {
	return time.Now().Format("15:04:05")
}

func utcNow() time.Time {
	return time.Now().UTC()
}
