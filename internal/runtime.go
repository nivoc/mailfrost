package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	Config     Config
	Paths      StatePaths
	RunID      string
	RunLogPath string
	runLog     *os.File
	lockFile   *os.File
}

func StartRuntime(config Config) (*Runtime, error) {
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
		Config:     config,
		Paths:      paths,
		RunID:      runID,
		RunLogPath: runLogPath,
		runLog:     runLog,
		lockFile:   lockFile,
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
	r.LogFile("INFO", fmt.Sprintf("Running command: %s", strings.Join(command, " ")))

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
	copyOutput := func(pipe io.ReadCloser) {
		defer wg.Done()
		data, readErr := io.ReadAll(pipe)
		if len(data) > 0 {
			mu.Lock()
			_, _ = output.Write(data)
			mu.Unlock()
			r.LogFileRaw(string(data))
		}
		if readErr != nil {
			r.LogFile("WARN", fmt.Sprintf("Read command output failed for %s: %v", strings.Join(command, " "), readErr))
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
	for {
		select {
		case err = <-done:
			wg.Wait()
			goto finished
		case <-ticker.C:
			elapsed := time.Since(startedAt).Round(time.Second)
			message := fmt.Sprintf("Still running: %s (%s elapsed)", strings.Join(command, " "), elapsed)
			r.Console(message)
			r.LogFile("INFO", message)
		}
	}

finished:
	outputText := output.String()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return outputText, fmt.Errorf("command failed with exit code %d: %s", exitErr.ExitCode(), strings.Join(command, " "))
		}
		return outputText, fmt.Errorf("run command %s: %w", strings.Join(command, " "), err)
	}
	return outputText, nil
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

func timestampLocal() string {
	return time.Now().Format("2006-01-02T15:04:05-0700")
}

func consoleTimestamp() string {
	return time.Now().Format("15:04:05")
}

func utcNow() time.Time {
	return time.Now().UTC()
}
