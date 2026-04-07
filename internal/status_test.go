package internal

import (
	"path/filepath"
	"testing"
)

func TestRegisterInstancePathsStoresRegistryInXDGConfigHome(t *testing.T) {
	xdgDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdgDir)

	stateDir := filepath.Join(t.TempDir(), "state")
	if err := RegisterInstancePaths("./config", "./.env", stateDir, "./maildir", "user@example.com"); err != nil {
		t.Fatalf("RegisterInstancePaths() error = %v", err)
	}
	if err := RegisterInstancePaths("./config-2", "./.env-2", stateDir, "./maildir-2", "updated@example.com"); err != nil {
		t.Fatalf("RegisterInstancePaths() update error = %v", err)
	}

	instances, err := ListRegisteredInstances()
	if err != nil {
		t.Fatalf("ListRegisteredInstances() error = %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("len(instances) = %d, want 1", len(instances))
	}
	if instances[0].Account != "updated@example.com" {
		t.Fatalf("Account = %q", instances[0].Account)
	}
	if instances[0].StateDir != filepath.Clean(stateDir) {
		t.Fatalf("StateDir = %q", instances[0].StateDir)
	}
}

func TestRuntimeStatusLifecycleWritesCurrentAndCompletedRun(t *testing.T) {
	xdgDir := t.TempDir()
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdgDir)

	config := Config{
		ConfigPath:  filepath.Join(root, "config"),
		EnvPath:     filepath.Join(root, ".env"),
		StateDir:    filepath.Join(root, "data", "state"),
		MaildirPath: filepath.Join(root, "data", "maildir"),
		Env: map[string]string{
			"IMAP_USERNAME": "user@example.com",
		},
	}

	runtime, err := StartRuntime(config, "backup")
	if err != nil {
		t.Fatalf("StartRuntime() error = %v", err)
	}
	defer runtime.Close()

	started, err := LoadStatusFromStateDir(config.StateDir)
	if err != nil {
		t.Fatalf("LoadStatusFromStateDir() after start error = %v", err)
	}
	if started.Current == nil {
		t.Fatalf("Current = nil, want running status")
	}
	if started.Current.Status != "running" {
		t.Fatalf("Current.Status = %q", started.Current.Status)
	}
	if started.Last != nil {
		t.Fatalf("Last = %#v, want nil", started.Last)
	}

	runtime.MarkCompleted("ok", 0, nil)

	completed, err := LoadStatusFromStateDir(config.StateDir)
	if err != nil {
		t.Fatalf("LoadStatusFromStateDir() after completion error = %v", err)
	}
	if completed.Current != nil {
		t.Fatalf("Current = %#v, want nil", completed.Current)
	}
	if completed.Last == nil {
		t.Fatalf("Last = nil, want completed status")
	}
	if completed.Last.Status != "ok" {
		t.Fatalf("Last.Status = %q", completed.Last.Status)
	}
	if completed.Last.Command != "backup" {
		t.Fatalf("Last.Command = %q", completed.Last.Command)
	}
	if completed.Last.LogPath != runtime.RunLogPath {
		t.Fatalf("Last.LogPath = %q, want %q", completed.Last.LogPath, runtime.RunLogPath)
	}
}

func TestCollectStatusViewsFallsBackToRegistryOutsideInstanceDir(t *testing.T) {
	xdgDir := t.TempDir()
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdgDir)

	stateDir := filepath.Join(root, "state")
	if err := RegisterInstancePaths("", "", stateDir, filepath.Join(root, "maildir"), "user@example.com"); err != nil {
		t.Fatalf("RegisterInstancePaths() error = %v", err)
	}
	if err := saveInstanceStatus(filepath.Join(stateDir, "status.json"), InstanceStatusSnapshot{
		SchemaVersion: statusSchemaVersion,
		Last: &RunStatus{
			Command:    "backup",
			Status:     "ok",
			StartedAt:  "2026-04-07T10:00:00Z",
			FinishedAt: "2026-04-07T10:02:00Z",
			LogPath:    filepath.Join(stateDir, "logs", "run-1.log"),
		},
	}); err != nil {
		t.Fatalf("saveInstanceStatus() error = %v", err)
	}

	views, err := CollectStatusViews(filepath.Join(root, "missing-config"), filepath.Join(root, "missing-env"))
	if err != nil {
		t.Fatalf("CollectStatusViews() error = %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("len(views) = %d, want 1", len(views))
	}
	if views[0].Instance.StateDir != filepath.Clean(stateDir) {
		t.Fatalf("StateDir = %q", views[0].Instance.StateDir)
	}
	if views[0].Status.Last == nil || views[0].Status.Last.Status != "ok" {
		t.Fatalf("Last = %#v, want completed ok status", views[0].Status.Last)
	}
}
