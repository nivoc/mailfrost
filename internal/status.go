package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const statusSchemaVersion = 1

type InstanceRegistry struct {
	Instances []RegisteredInstance `json:"instances"`
}

type RegisteredInstance struct {
	ConfigPath  string `json:"config_path,omitempty"`
	EnvPath     string `json:"env_path,omitempty"`
	StateDir    string `json:"state_dir"`
	MaildirPath string `json:"maildir_path,omitempty"`
	Account     string `json:"account,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

type InstanceStatusSnapshot struct {
	SchemaVersion int        `json:"schema_version"`
	UpdatedAt     string     `json:"updated_at,omitempty"`
	Current       *RunStatus `json:"current,omitempty"`
	Last          *RunStatus `json:"last,omitempty"`
}

type RunStatus struct {
	RunID      string `json:"run_id,omitempty"`
	Command    string `json:"command,omitempty"`
	Status     string `json:"status,omitempty"`
	PID        int    `json:"pid,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	ExitCode   int    `json:"exit_code"`
	LogPath    string `json:"log_path,omitempty"`
	Error      string `json:"error,omitempty"`
}

type InstanceStatusView struct {
	Instance RegisteredInstance
	Status   InstanceStatusSnapshot
}

func RegisterInstance(config Config) error {
	return RegisterInstancePaths(config.ConfigPath, config.EnvPath, config.StateDir, config.MaildirPath, strings.TrimSpace(config.Env["IMAP_USERNAME"]))
}

func RegisterInstancePaths(configPath, envPath, stateDir, maildirPath, account string) error {
	stateDir = strings.TrimSpace(stateDir)
	if stateDir == "" {
		return fmt.Errorf("state dir must not be empty")
	}

	resolvedStateDir, err := filepath.Abs(stateDir)
	if err != nil {
		return fmt.Errorf("resolve state dir: %w", err)
	}

	entry := RegisteredInstance{
		ConfigPath:  normalizeOptionalPath(configPath),
		EnvPath:     normalizeOptionalPath(envPath),
		StateDir:    filepath.Clean(resolvedStateDir),
		MaildirPath: normalizeOptionalPath(maildirPath),
		Account:     strings.TrimSpace(account),
		UpdatedAt:   utcNow().Format(time.RFC3339),
	}

	registryPath, err := registryFilePath()
	if err != nil {
		return err
	}
	lockFile, err := openLockFile(registryPath + ".lock")
	if err != nil {
		return err
	}
	defer closeLockFile(lockFile)

	registry, err := loadRegistry(registryPath)
	if err != nil {
		return err
	}

	updated := false
	for i := range registry.Instances {
		if filepath.Clean(registry.Instances[i].StateDir) != entry.StateDir {
			continue
		}
		mergeRegisteredInstance(&registry.Instances[i], entry)
		updated = true
		break
	}
	if !updated {
		registry.Instances = append(registry.Instances, entry)
	}

	sort.Slice(registry.Instances, func(i, j int) bool {
		left := registrySortKey(registry.Instances[i])
		right := registrySortKey(registry.Instances[j])
		return left < right
	})

	return writeJSONFile(registryPath, registry)
}

func CollectStatusViews(configPath, envPath string) ([]InstanceStatusView, error) {
	if local, err := collectLocalStatusView(configPath, envPath); err != nil {
		return nil, err
	} else if local != nil {
		return []InstanceStatusView{*local}, nil
	}

	instances, err := ListRegisteredInstances()
	if err != nil {
		return nil, err
	}
	views := make([]InstanceStatusView, 0, len(instances))
	for _, instance := range instances {
		snapshot, err := LoadStatusFromStateDir(instance.StateDir)
		if err != nil {
			return nil, err
		}
		views = append(views, InstanceStatusView{Instance: instance, Status: snapshot})
	}
	return views, nil
}

func ListRegisteredInstances() ([]RegisteredInstance, error) {
	registryPath, err := registryFilePath()
	if err != nil {
		return nil, err
	}
	registry, err := loadRegistry(registryPath)
	if err != nil {
		return nil, err
	}
	instances := append([]RegisteredInstance(nil), registry.Instances...)
	return instances, nil
}

func LoadStatusFromStateDir(stateDir string) (InstanceStatusSnapshot, error) {
	return loadInstanceStatus(filepath.Join(stateDir, "status.json"))
}

func RenderStatusViews(views []InstanceStatusView) string {
	if len(views) == 0 {
		return "No registered Mailfrost instances.\nRun `mailfrost backup` from an instance once so it can be discovered.\n"
	}

	var lines []string
	for i, view := range views {
		if i > 0 {
			lines = append(lines, "")
		}

		title := strings.TrimSpace(view.Instance.Account)
		if title == "" {
			title = filepath.Base(view.Instance.StateDir)
		}
		lines = append(lines, title)
		lines = append(lines, fmt.Sprintf("status: %s", describeCurrentStatus(view.Status)))
		lines = append(lines, fmt.Sprintf("state dir: %s", view.Instance.StateDir))
		if strings.TrimSpace(view.Instance.MaildirPath) != "" {
			lines = append(lines, fmt.Sprintf("maildir: %s", view.Instance.MaildirPath))
		}
		if strings.TrimSpace(view.Instance.ConfigPath) != "" {
			lines = append(lines, fmt.Sprintf("config: %s", view.Instance.ConfigPath))
		}
		if strings.TrimSpace(view.Instance.EnvPath) != "" {
			lines = append(lines, fmt.Sprintf("env: %s", view.Instance.EnvPath))
		}

		if current := view.Status.Current; current != nil {
			lines = append(lines, fmt.Sprintf("current run: %s", describeCurrentRun(*current)))
			if strings.TrimSpace(current.LogPath) != "" {
				lines = append(lines, fmt.Sprintf("current log: %s", current.LogPath))
			}
		}

		if last := view.Status.Last; last != nil {
			lines = append(lines, fmt.Sprintf("last run: %s", describeCompletedRun(*last)))
			if strings.TrimSpace(last.LogPath) != "" {
				lines = append(lines, fmt.Sprintf("last log: %s", last.LogPath))
			}
		} else if view.Status.Current == nil {
			lines = append(lines, "last run: none recorded")
		}
	}

	return strings.Join(lines, "\n") + "\n"
}

func collectLocalStatusView(configPath, envPath string) (*InstanceStatusView, error) {
	resolvedEnvPath, err := filepath.Abs(envPath)
	if err != nil {
		return nil, fmt.Errorf("resolve .env path: %w", err)
	}
	if _, err := os.Stat(resolvedEnvPath); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat .env file: %w", err)
	}

	config, err := loadConfig(configPath, envPath)
	if err != nil {
		return nil, err
	}
	snapshot, err := LoadStatusFromStateDir(config.StateDir)
	if err != nil {
		return nil, err
	}
	return &InstanceStatusView{
		Instance: RegisteredInstance{
			ConfigPath:  config.ConfigPath,
			EnvPath:     config.EnvPath,
			StateDir:    config.StateDir,
			MaildirPath: config.MaildirPath,
			Account:     strings.TrimSpace(config.Env["IMAP_USERNAME"]),
		},
		Status: snapshot,
	}, nil
}

func registryFilePath() (string, error) {
	baseDir := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME"))
	if baseDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir: %w", err)
		}
		baseDir = filepath.Join(homeDir, ".config")
	}
	return filepath.Join(baseDir, defaultToolName, "registry.json"), nil
}

func loadRegistry(path string) (InstanceRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return InstanceRegistry{}, nil
		}
		return InstanceRegistry{}, fmt.Errorf("read registry: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return InstanceRegistry{}, nil
	}

	var registry InstanceRegistry
	if err := json.Unmarshal(data, &registry); err != nil {
		return InstanceRegistry{}, fmt.Errorf("parse registry: %w", err)
	}
	return registry, nil
}

func loadInstanceStatus(path string) (InstanceStatusSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return InstanceStatusSnapshot{SchemaVersion: statusSchemaVersion}, nil
		}
		return InstanceStatusSnapshot{}, fmt.Errorf("read status file: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return InstanceStatusSnapshot{SchemaVersion: statusSchemaVersion}, nil
	}

	var snapshot InstanceStatusSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return InstanceStatusSnapshot{}, fmt.Errorf("parse status file: %w", err)
	}
	if snapshot.SchemaVersion == 0 {
		snapshot.SchemaVersion = statusSchemaVersion
	}
	return snapshot, nil
}

func saveInstanceStatus(path string, snapshot InstanceStatusSnapshot) error {
	snapshot.SchemaVersion = statusSchemaVersion
	return writeJSONFile(path, snapshot)
}

func writeJSONFile(path string, payload any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tempFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if _, err := tempFile.Write(data); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Chmod(0o644); err != nil {
		_ = tempFile.Close()
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

func mergeRegisteredInstance(dst *RegisteredInstance, src RegisteredInstance) {
	if src.ConfigPath != "" {
		dst.ConfigPath = src.ConfigPath
	}
	if src.EnvPath != "" {
		dst.EnvPath = src.EnvPath
	}
	if src.StateDir != "" {
		dst.StateDir = src.StateDir
	}
	if src.MaildirPath != "" {
		dst.MaildirPath = src.MaildirPath
	}
	if src.Account != "" {
		dst.Account = src.Account
	}
	dst.UpdatedAt = src.UpdatedAt
}

func registrySortKey(instance RegisteredInstance) string {
	account := strings.ToLower(strings.TrimSpace(instance.Account))
	stateDir := strings.ToLower(strings.TrimSpace(instance.StateDir))
	return account + "\x00" + stateDir
}

func normalizeOptionalPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	resolvedPath, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(resolvedPath)
}

func describeCurrentStatus(snapshot InstanceStatusSnapshot) string {
	if snapshot.Current != nil {
		if isRunLive(*snapshot.Current) {
			return "RUNNING"
		}
		return "STALE"
	}
	if snapshot.Last != nil {
		return strings.ToUpper(strings.TrimSpace(snapshot.Last.Status))
	}
	return "UNKNOWN"
}

func describeCurrentRun(run RunStatus) string {
	if isRunLive(run) {
		return fmt.Sprintf("%s since %s (pid %d)", strings.ToUpper(strings.TrimSpace(run.Command)), formatStatusTime(run.StartedAt), run.PID)
	}
	return fmt.Sprintf("stale %s record from %s (pid %d)", strings.TrimSpace(run.Command), formatStatusTime(run.StartedAt), run.PID)
}

func describeCompletedRun(run RunStatus) string {
	label := strings.ToUpper(strings.TrimSpace(run.Status))
	command := strings.TrimSpace(run.Command)
	if command == "" {
		command = "run"
	}
	at := run.FinishedAt
	if strings.TrimSpace(at) == "" {
		at = run.StartedAt
	}
	return fmt.Sprintf("%s at %s via %s", label, formatStatusTime(at), command)
}

func formatStatusTime(raw string) string {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return strings.TrimSpace(raw)
	}
	return parsed.Local().Format("2006-01-02 15:04:05")
}

func isRunLive(run RunStatus) bool {
	if !strings.EqualFold(strings.TrimSpace(run.Status), "running") {
		return false
	}
	if run.PID <= 0 {
		return false
	}
	err := syscall.Kill(run.PID, 0)
	return err == nil || err == syscall.EPERM
}

func openLockFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock file: %w", err)
	}
	return file, nil
}

func closeLockFile(file *os.File) {
	if file == nil {
		return
	}
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}
