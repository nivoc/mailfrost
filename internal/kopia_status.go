package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type kopiaRepoStatus struct {
	RepoType                string
	Storage                 string
	Connected               string
	Encryption              string
	Compression             string
	ComplianceHold          string
	MaintenanceIntervalDays int
	LastMaintenance         string
	NextMaintenanceDue      string
}

func renderKopiaRepoStatus(status kopiaRepoStatus) string {
	lines := []string{
		"",
		"KOPIA REPOSITORY",
		"",
		fmt.Sprintf("type: %s", status.RepoType),
		fmt.Sprintf("storage: %s", status.Storage),
		fmt.Sprintf("connected: %s", status.Connected),
		"",
		fmt.Sprintf("encryption: %s", status.Encryption),
		fmt.Sprintf("compression: %s", status.Compression),
		fmt.Sprintf("object lock / compliance hold: %s", status.ComplianceHold),
		fmt.Sprintf("maintenance: %s", formatMaintenanceInterval(status.MaintenanceIntervalDays)),
		fmt.Sprintf("last maintenance: %s", status.LastMaintenance),
		fmt.Sprintf("next maintenance due: %s", status.NextMaintenanceDue),
		"",
	}
	return strings.Join(lines, "\n")
}

func buildKopiaRepoStatusFromConfig(config Config, paths StatePaths, compression string) kopiaRepoStatus {
	return kopiaRepoStatus{
		RepoType:                config.KopiaRepoType,
		Storage:                 kopiaStorageDescription(config),
		Connected:               "yes",
		Encryption:              "configured",
		Compression:             compression,
		ComplianceHold:          kopiaComplianceDescription(config.Env),
		MaintenanceIntervalDays: config.KopiaMaintenanceIntervalDays,
		LastMaintenance:         formatLastMaintenance(paths.KopiaMaintenanceStamp),
		NextMaintenanceDue:      formatNextMaintenanceDue(paths.KopiaMaintenanceStamp, config.KopiaMaintenanceIntervalDays),
	}
}

func buildKopiaRepoStatusFromSetup(values map[string]string, stateDir string, maintenanceIntervalDays int, compression string) kopiaRepoStatus {
	cfg := Config{
		KopiaRepoType:                strings.TrimSpace(values["KOPIA_REPO_TYPE"]),
		KopiaRepoPath:                resolvePath(filepath.Dir(stateDir), values["KOPIA_REPO_PATH"]),
		KopiaS3Bucket:                strings.TrimSpace(values["KOPIA_S3_BUCKET"]),
		KopiaS3Endpoint:              strings.TrimSpace(values["KOPIA_S3_ENDPOINT"]),
		KopiaS3Prefix:                strings.TrimSpace(values["KOPIA_S3_PREFIX"]),
		KopiaMaintenanceIntervalDays: maintenanceIntervalDays,
		Env:                          map[string]string{},
	}
	for key, value := range values {
		cfg.Env[key] = value
	}
	return buildKopiaRepoStatusFromConfig(cfg, StatePathsFromDir(stateDir), compression)
}

func kopiaStorageDescription(config Config) string {
	switch config.KopiaRepoType {
	case "filesystem":
		return config.KopiaRepoPath
	case "s3":
		storage := fmt.Sprintf("s3://%s", config.KopiaS3Bucket)
		if prefix := strings.TrimSpace(config.KopiaS3Prefix); prefix != "" {
			storage += "/" + strings.Trim(prefix, "/")
		}
		if endpoint := strings.TrimSpace(config.KopiaS3Endpoint); endpoint != "" {
			storage += fmt.Sprintf(" (%s)", endpoint)
		}
		return storage
	default:
		return "unknown"
	}
}

func kopiaComplianceDescription(env map[string]string) string {
	mode := strings.ToUpper(strings.TrimSpace(env["KOPIA_S3_OBJECT_LOCK_MODE"]))
	days := strings.TrimSpace(env["KOPIA_S3_OBJECT_LOCK_DAYS"])
	if mode == "" {
		return "off"
	}
	if days == "" {
		return mode
	}
	return fmt.Sprintf("%s, %s days", mode, days)
}

func formatMaintenanceInterval(days int) string {
	if days <= 0 {
		return "off"
	}
	if days == 1 {
		return "every 1 day"
	}
	return fmt.Sprintf("every %d days", days)
}

func formatLastMaintenance(stampPath string) string {
	info, err := os.Stat(stampPath)
	if err != nil {
		return "never"
	}
	return info.ModTime().Local().Format("2006-01-02 15:04")
}

func formatNextMaintenanceDue(stampPath string, intervalDays int) string {
	if intervalDays <= 0 {
		return "off"
	}
	info, err := os.Stat(stampPath)
	if err != nil {
		return "now"
	}
	nextDue := info.ModTime().Add(time.Duration(intervalDays) * 24 * time.Hour)
	if time.Now().After(nextDue) {
		return "now"
	}
	return nextDue.Local().Format("2006-01-02 15:04")
}

func parseKopiaCompressionPolicyShow(output string) string {
	lines := strings.Split(output, "\n")
	inCompressionSection := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "Compression disabled.":
			return "disabled"
		case trimmed == "Compression:":
			inCompressionSection = true
		case inCompressionSection && strings.HasPrefix(trimmed, "Compressor:"):
			value := strings.TrimSpace(strings.TrimPrefix(trimmed, "Compressor:"))
			if value != "" {
				return value
			}
		case inCompressionSection && trimmed == "":
			inCompressionSection = false
		}
	}
	return "unknown"
}
