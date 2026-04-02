package internal

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

const (
	defaultToolName                 = "mailfrost"
	defaultRecoverChannelName       = "mailfrost-recover"
	currentKopiaPurposeTag          = "purpose:mailfrost"
	legacyKopiaPurposeTag           = "purpose:mail-backup"
	defaultMaildirPath              = "./data/maildir"
	defaultStateDir                 = "./data/state"
	defaultKopiaConfigPath          = "./data/kopia/repository.config"
	defaultReportSampleLimit        = 20
	defaultImmutabilityDays         = 30
	defaultKopiaMaintenanceInterval = 7
	defaultIgnoreMailboxRegex       = `(^|[/.])(Trash|Junk|Spam|Drafts)([/.]|$)`
	defaultMbsyncConfigPath         = "./data/state/mbsyncrc.generated"
)

var (
	defaultMbsyncCommand     = []string{"mbsync", "-c", "./data/state/mbsyncrc.generated", defaultToolName}
	defaultKopiaCommand      = []string{"kopia"}
	defaultKopiaSnapshotArgs = []string{"--tags", currentKopiaPurposeTag}
)

type Config struct {
	ConfigPath                   string
	ConfigFileLoaded             bool
	AdvancedConfigPath           string
	AdvancedConfigFileLoaded     bool
	EnvPath                      string
	MaildirPath                  string
	StateDir                     string
	ReportSampleLimit            int
	ImmutabilityDays             int
	IgnoreMailboxRegex           string
	KopiaConfigPath              string
	KopiaPassword                string
	KopiaRepoType                string
	KopiaRepoPath                string
	KopiaS3Bucket                string
	KopiaS3Endpoint              string
	KopiaS3Prefix                string
	KopiaMaintenanceIntervalDays int
	KopiaIncludeStateDir         bool
	MbsyncConfigPath             string
	MbsyncCommand                []string
	KopiaCommand                 []string
	KopiaSnapshotArgs            []string
	AlertCommand                 string
	Env                          map[string]string
}

func loadConfig(configPath, envPath string) (Config, error) {
	resolvedConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("resolve config path: %w", err)
	}
	resolvedEnvPath, err := filepath.Abs(envPath)
	if err != nil {
		return Config{}, fmt.Errorf("resolve .env path: %w", err)
	}

	configBaseDir := filepath.Dir(resolvedConfigPath)
	advancedConfigPath := filepath.Join(configBaseDir, "config.advanced")

	advancedConfigExists, err := statOptionalFile(advancedConfigPath, "advanced config")
	if err != nil {
		return Config{}, err
	}
	configExists, err := statOptionalFile(resolvedConfigPath, "config")
	if err != nil {
		return Config{}, err
	}
	if err := statRequiredFile(resolvedEnvPath, ".env"); err != nil {
		return Config{}, err
	}

	cfg := Config{
		ConfigPath:                   resolvedConfigPath,
		ConfigFileLoaded:             configExists,
		AdvancedConfigPath:           advancedConfigPath,
		AdvancedConfigFileLoaded:     advancedConfigExists,
		EnvPath:                      resolvedEnvPath,
		MaildirPath:                  resolvePath(configBaseDir, defaultMaildirPath),
		StateDir:                     resolvePath(configBaseDir, defaultStateDir),
		ReportSampleLimit:            defaultReportSampleLimit,
		ImmutabilityDays:             defaultImmutabilityDays,
		IgnoreMailboxRegex:           defaultIgnoreMailboxRegex,
		KopiaConfigPath:              resolvePath(filepath.Dir(resolvedEnvPath), defaultKopiaConfigPath),
		KopiaMaintenanceIntervalDays: defaultKopiaMaintenanceInterval,
		KopiaIncludeStateDir:         true,
		MbsyncConfigPath:             resolvePath(configBaseDir, defaultMbsyncConfigPath),
		MbsyncCommand:                append([]string(nil), defaultMbsyncCommand...),
		KopiaCommand:                 append([]string(nil), defaultKopiaCommand...),
		KopiaSnapshotArgs:            append([]string(nil), defaultKopiaSnapshotArgs...),
		Env:                          map[string]string{},
	}

	if advancedConfigExists {
		if err := parseKeyValueFile(advancedConfigPath, "advanced config", func(lineNo int, key, value string) error {
			return applyConfigValue(&cfg, configBaseDir, lineNo, key, value)
		}); err != nil {
			return Config{}, err
		}
	}

	if configExists {
		if err := parseKeyValueFile(resolvedConfigPath, "config", func(lineNo int, key, value string) error {
			return applyConfigValue(&cfg, configBaseDir, lineNo, key, value)
		}); err != nil {
			return Config{}, err
		}
	}

	if err := parseKeyValueFile(resolvedEnvPath, ".env", func(lineNo int, key, value string) error {
		return applyEnvValue(&cfg, filepath.Dir(resolvedEnvPath), lineNo, key, value)
	}); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func LoadConfig(configPath, envPath string) (Config, error) {
	cfg, err := loadConfig(configPath, envPath)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func LoadSetupConfig(envPath string) (map[string]string, error) {
	resolvedEnvPath, err := filepath.Abs(envPath)
	if err != nil {
		return nil, fmt.Errorf("resolve .env path: %w", err)
	}
	values := map[string]string{}
	if _, err := os.Stat(resolvedEnvPath); err != nil {
		return values, nil
	}
	if err := parseKeyValueFile(resolvedEnvPath, ".env", func(lineNo int, key, value string) error {
		scalar, err := parseEnvValue(value)
		if err != nil {
			return fmt.Errorf("invalid %s in .env line %d: %w", key, lineNo, err)
		}
		values[key] = scalar
		return nil
	}); err != nil {
		return nil, err
	}
	return values, nil
}

func normalizedAccountTag(username string) string {
	username = strings.ToLower(strings.TrimSpace(username))
	if username == "" {
		return "unknown-account"
	}
	var builder strings.Builder
	lastDash := false
	for _, r := range username {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	value := strings.Trim(builder.String(), "-")
	if value == "" {
		return "unknown-account"
	}
	return value
}

func (c Config) Validate() error {
	if c.MaildirPath == "" {
		return fmt.Errorf("maildir_path must not be empty")
	}
	if info, err := os.Stat(c.MaildirPath); err == nil && !info.IsDir() {
		return fmt.Errorf("maildir_path is not a directory: %s", c.MaildirPath)
	}
	if info, err := os.Stat(c.StateDir); err == nil && !info.IsDir() {
		return fmt.Errorf("state_dir is not a directory: %s", c.StateDir)
	}
	if c.KopiaConfigPath == "" {
		return fmt.Errorf("KOPIA_CONFIG_PATH must be set in .env")
	}
	if c.KopiaPassword == "" {
		return fmt.Errorf("KOPIA_PASSWORD must be set in .env")
	}
	if c.KopiaRepoType == "" {
		return fmt.Errorf("KOPIA_REPO_TYPE must be set in .env")
	}
	switch c.KopiaRepoType {
	case "filesystem":
		if c.KopiaRepoPath == "" {
			return fmt.Errorf("KOPIA_REPO_PATH must be set for filesystem repositories")
		}
	case "s3":
		if c.KopiaS3Bucket == "" {
			return fmt.Errorf("KOPIA_S3_BUCKET must be set for s3 repositories")
		}
		if c.KopiaS3Endpoint == "" {
			return fmt.Errorf("KOPIA_S3_ENDPOINT must be set for s3 repositories")
		}
	default:
		return fmt.Errorf("KOPIA_REPO_TYPE must be one of filesystem or s3")
	}
	if c.ReportSampleLimit < 1 {
		return fmt.Errorf("report_sample_limit must be an integer >= 1")
	}
	if c.ImmutabilityDays < 1 {
		return fmt.Errorf("immutability_days must be an integer >= 1")
	}
	if c.KopiaMaintenanceIntervalDays < 0 {
		return fmt.Errorf("kopia_maintenance_interval_days must be an integer >= 0")
	}
	if c.MbsyncConfigPath == "" {
		return fmt.Errorf("mbsync_config_path must not be empty")
	}
	if info, err := os.Stat(c.MbsyncConfigPath); err == nil && info.IsDir() {
		return fmt.Errorf("mbsync_config_path is a directory: %s", c.MbsyncConfigPath)
	}
	if len(c.MbsyncCommand) == 0 {
		return fmt.Errorf("mbsync_command must not be empty")
	}
	if len(c.KopiaCommand) == 0 {
		return fmt.Errorf("kopia_command must not be empty")
	}
	if _, err := regexp.Compile(c.IgnoreMailboxRegex); err != nil {
		return fmt.Errorf("ignore_mailbox_regex is invalid: %w", err)
	}
	if err := validateCommand(c.MbsyncCommand); err != nil {
		return err
	}
	if err := validateCommand(c.KopiaCommand); err != nil {
		return err
	}
	return nil
}

func statOptionalFile(path, label string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat %s file: %w", label, err)
	}
	if info.IsDir() {
		return false, fmt.Errorf("%s file is a directory: %s", label, path)
	}
	return true, nil
}

func statRequiredFile(path, label string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s file not found: %s", label, path)
		}
		return fmt.Errorf("stat %s file: %w", label, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%s file is a directory: %s", label, path)
	}
	return nil
}

func parseKeyValueFile(path, label string, apply func(lineNo int, key, value string) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s file: %w", label, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(stripComments(scanner.Text()))
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("invalid %s line %d: %s", label, lineNo, line)
		}
		key = strings.ToUpper(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if err := apply(lineNo, key, value); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s file: %w", label, err)
	}
	return nil
}

func applyConfigValue(cfg *Config, baseDir string, lineNo int, key, value string) error {
	switch key {
	case "MAILDIR_PATH":
		scalar, err := parseEnvValue(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.MaildirPath = resolvePath(baseDir, scalar)
	case "STATE_DIR":
		scalar, err := parseEnvValue(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.StateDir = resolvePath(baseDir, scalar)
	case "REPORT_SAMPLE_LIMIT":
		number, err := parseInt(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.ReportSampleLimit = number
	case "IMMUTABILITY_DAYS":
		number, err := parseInt(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.ImmutabilityDays = number
	case "IGNORE_MAILBOX_REGEX":
		scalar, err := parseEnvValue(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.IgnoreMailboxRegex = scalar
	case "KOPIA_MAINTENANCE_INTERVAL_DAYS":
		number, err := parseInt(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.KopiaMaintenanceIntervalDays = number
	case "KOPIA_INCLUDE_STATE_DIR":
		boolean, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.KopiaIncludeStateDir = boolean
	case "MBSYNC_CONFIG_PATH":
		scalar, err := parseEnvValue(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.MbsyncConfigPath = resolvePath(baseDir, scalar)
	case "MBSYNC_COMMAND":
		command, err := parseCommandLine(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.MbsyncCommand = resolveCommand(baseDir, command)
	case "KOPIA_COMMAND":
		command, err := parseCommandLine(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.KopiaCommand = resolveCommand(baseDir, command)
	case "KOPIA_SNAPSHOT_ARGS":
		values, err := parseCommandLine(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.KopiaSnapshotArgs = expandStringSlice(values)
	case "ALERT_COMMAND":
		scalar, err := parseEnvValue(value)
		if err != nil {
			return fmt.Errorf("invalid %s in config line %d: %w", key, lineNo, err)
		}
		cfg.AlertCommand = strings.TrimSpace(scalar)
	case "KOPIA_CONFIG_PATH", "KOPIA_PASSWORD", "KOPIA_REPO_TYPE", "KOPIA_REPO_PATH", "KOPIA_S3_BUCKET", "KOPIA_S3_ENDPOINT", "KOPIA_S3_PREFIX":
		return fmt.Errorf("%s belongs in .env, not config (line %d)", key, lineNo)
	default:
		return fmt.Errorf("unknown config key on line %d: %s", lineNo, key)
	}
	return nil
}

func applyEnvValue(cfg *Config, baseDir string, lineNo int, key, value string) error {
	if isRuntimeConfigKey(key) {
		return fmt.Errorf("%s belongs in config, not .env (line %d)", key, lineNo)
	}
	scalar, err := parseEnvValue(value)
	if err != nil {
		return fmt.Errorf("invalid %s in .env line %d: %w", key, lineNo, err)
	}

	switch key {
	case "KOPIA_CONFIG_PATH":
		cfg.KopiaConfigPath = resolvePath(baseDir, scalar)
	case "KOPIA_PASSWORD":
		cfg.KopiaPassword = scalar
	case "KOPIA_REPO_TYPE":
		cfg.KopiaRepoType = scalar
		cfg.Env[key] = scalar
	case "KOPIA_REPO_PATH":
		cfg.KopiaRepoPath = resolvePath(baseDir, scalar)
		cfg.Env[key] = cfg.KopiaRepoPath
	case "KOPIA_S3_BUCKET":
		cfg.KopiaS3Bucket = scalar
		cfg.Env[key] = scalar
	case "KOPIA_S3_ENDPOINT":
		cfg.KopiaS3Endpoint = scalar
		cfg.Env[key] = scalar
	case "KOPIA_S3_PREFIX":
		cfg.KopiaS3Prefix = scalar
		cfg.Env[key] = scalar
	default:
		cfg.Env[key] = resolveExternalEnvValue(baseDir, scalar)
	}
	return nil
}

func isRuntimeConfigKey(key string) bool {
	switch key {
	case "MAILDIR_PATH",
		"STATE_DIR",
		"REPORT_SAMPLE_LIMIT",
		"IMMUTABILITY_DAYS",
		"IGNORE_MAILBOX_REGEX",
		"KOPIA_MAINTENANCE_INTERVAL_DAYS",
		"KOPIA_INCLUDE_STATE_DIR",
		"MBSYNC_CONFIG_PATH",
		"MBSYNC_COMMAND",
		"KOPIA_COMMAND",
		"KOPIA_SNAPSHOT_ARGS",
		"ALERT_COMMAND":
		return true
	default:
		return false
	}
}

func stripComments(line string) string {
	inDouble := false
	inSingle := false
	escaped := false
	for i, r := range line {
		switch {
		case escaped:
			escaped = false
		case r == '\\' && !inSingle:
			escaped = true
		case r == '"' && !inSingle:
			inDouble = !inDouble
		case r == '\'' && !inDouble:
			inSingle = !inSingle
		case r == '#' && !inDouble && !inSingle:
			return line[:i]
		}
	}
	return line
}

func parseEnvValue(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if strings.HasPrefix(value, "\"") || strings.HasPrefix(value, "'") {
		unquoted, err := strconv.Unquote(convertSingleQuoted(value))
		if err != nil {
			return "", fmt.Errorf("expected string value")
		}
		return unquoted, nil
	}
	return value, nil
}

func parseInt(value string) (int, error) {
	scalar, err := parseEnvValue(value)
	if err != nil {
		return 0, err
	}
	number, err := strconv.Atoi(scalar)
	if err != nil {
		return 0, fmt.Errorf("expected integer")
	}
	return number, nil
}

func parseBool(value string) (bool, error) {
	scalar, err := parseEnvValue(value)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(scalar) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("expected boolean")
	}
}

func parseCommandLine(value string) ([]string, error) {
	scalar, err := parseEnvValue(value)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(scalar) == "" {
		return []string{}, nil
	}
	return splitShellWords(scalar)
}

func splitShellWords(value string) ([]string, error) {
	var items []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	escaped := false

	for _, r := range value {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\' && !inSingle:
			escaped = true
		case r == '\'' && !inDouble:
			inSingle = !inSingle
		case r == '"' && !inSingle:
			inDouble = !inDouble
		case (r == ' ' || r == '\t') && !inSingle && !inDouble:
			if current.Len() > 0 {
				items = append(items, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if escaped || inSingle || inDouble {
		return nil, fmt.Errorf("unterminated quoted value")
	}
	if current.Len() > 0 {
		items = append(items, current.String())
	}
	return items, nil
}

func convertSingleQuoted(value string) string {
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		inner := strings.ReplaceAll(value[1:len(value)-1], `\`, `\\`)
		inner = strings.ReplaceAll(inner, `"`, `\"`)
		return `"` + inner + `"`
	}
	return value
}

func expandString(value string) string {
	value = os.ExpandEnv(value)
	if strings.HasPrefix(value, "~") {
		home, err := os.UserHomeDir()
		if err == nil {
			if value == "~" {
				value = home
			} else if strings.HasPrefix(value, "~/") {
				value = filepath.Join(home, value[2:])
			}
		}
	}
	return value
}

func expandStringSlice(values []string) []string {
	resolved := make([]string, 0, len(values))
	for _, value := range values {
		resolved = append(resolved, expandString(value))
	}
	return resolved
}

func resolveExternalEnvValue(baseDir, rawValue string) string {
	expanded := expandString(rawValue)
	if looksLikePath(expanded) {
		return resolvePath(baseDir, expanded)
	}
	return expanded
}

func looksLikePath(value string) bool {
	switch {
	case value == "":
		return false
	case strings.HasPrefix(value, "/"):
		return true
	case strings.HasPrefix(value, "./"):
		return true
	case strings.HasPrefix(value, "../"):
		return true
	case strings.HasPrefix(value, "~"):
		return true
	default:
		return false
	}
}

func resolvePath(baseDir, rawValue string) string {
	expanded := expandString(rawValue)
	if filepath.IsAbs(expanded) {
		return filepath.Clean(expanded)
	}
	return filepath.Clean(filepath.Join(baseDir, expanded))
}

func resolveCommand(baseDir string, rawValue []string) []string {
	command := expandStringSlice(rawValue)
	if len(command) == 0 {
		return command
	}
	if strings.Contains(command[0], "/") {
		command[0] = resolvePath(baseDir, command[0])
	}
	return command
}

func validateCommand(command []string) error {
	executable := command[0]
	if strings.Contains(executable, "/") {
		info, err := os.Stat(executable)
		if err != nil || info.IsDir() {
			return fmt.Errorf("command not found: %s", executable)
		}
		return nil
	}
	if _, err := exec.LookPath(executable); err != nil {
		return fmt.Errorf("command not found in PATH: %s", executable)
	}
	return nil
}
