package internal

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SetupApp struct {
	EnvPath string
	stdin   *bufio.Reader
}

type objectLockSettings struct {
	Enabled       bool
	RetentionMode string
	RetentionDays int
}

type s3ObjectLockConfiguration struct {
	ObjectLockEnabled string `xml:"ObjectLockEnabled"`
}

type s3ErrorResponse struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (a *SetupApp) Run() error {
	a.stdin = bufio.NewReader(os.Stdin)

	existing, err := LoadSetupConfig(a.EnvPath)
	if err != nil {
		return err
	}

	fmt.Println("This wizard stores IMAP settings in .env and generates the mbsync config for you.")
	fmt.Println()

	imapHost, err := a.prompt("IMAP host", existing["IMAP_HOST"], false)
	if err != nil {
		return err
	}
	imapUser, err := a.prompt("IMAP username", existing["IMAP_USERNAME"], false)
	if err != nil {
		return err
	}
	imapPassword, err := a.prompt("IMAP password", existing["IMAP_PASSWORD"], true)
	if err != nil {
		return err
	}
	imapPort, err := a.prompt("IMAP port", defaultIfEmpty(existing["IMAP_PORT"], "993"), false)
	if err != nil {
		return err
	}

	repoTypeDefault := defaultIfEmpty(existing["KOPIA_REPO_TYPE"], "filesystem")
	fmt.Println()
	fmt.Println("Kopia repository type:")
	if repoTypeDefault == "s3" {
		fmt.Println("  [1] filesystem (local directory)")
		fmt.Println("  [2] s3 (default)")
	} else {
		fmt.Println("  [1] filesystem (default)")
		fmt.Println("  [2] s3")
	}
	fmt.Println()
	repoChoice, err := a.prompt("Select repository type", defaultRepoChoice(repoTypeDefault), false)
	if err != nil {
		return err
	}
	repoType, err := parseRepoTypeChoice(repoChoice, repoTypeDefault)
	if err != nil {
		return err
	}

	kopiaConfigPath := defaultIfEmpty(existing["KOPIA_CONFIG_PATH"], "./data/kopia/repository.config")
	kopiaConfigPath, err = a.prompt("Kopia config path", kopiaConfigPath, false)
	if err != nil {
		return err
	}

	var repoPath, s3Bucket, s3Endpoint, s3Prefix, awsKeyID, awsSecret string
	s3PrefixCleared := false
	objectLock := objectLockSettings{}
	switch repoType {
	case "filesystem":
		repoPath, err = a.prompt("Repository path", defaultIfEmpty(existing["KOPIA_REPO_PATH"], "./data/kopia/repo"), false)
		if err != nil {
			return err
		}
	case "s3":
		s3Bucket, err = a.prompt("S3 bucket", existing["KOPIA_S3_BUCKET"], false)
		if err != nil {
			return err
		}
		s3Endpoint, err = a.prompt("S3 endpoint", existing["KOPIA_S3_ENDPOINT"], false)
		if err != nil {
			return err
		}
		s3Prefix, err = a.promptOptional("S3 prefix (optional, subpath inside bucket; enter / to clear)", existing["KOPIA_S3_PREFIX"], false)
		if err != nil {
			return err
		}
		s3PrefixCleared = strings.TrimSpace(s3Prefix) == ""
		s3Prefix = normalizeS3Prefix(s3Prefix)
		awsKeyID, err = a.prompt("AWS access key ID", existing["AWS_ACCESS_KEY_ID"], false)
		if err != nil {
			return err
		}
		awsSecret, err = a.prompt("AWS secret access key", existing["AWS_SECRET_ACCESS_KEY"], true)
		if err != nil {
			return err
		}
	}

	maildirPath, err := a.prompt("Maildir path", defaultMaildirPath, false)
	if err != nil {
		return err
	}
	stateDir, err := a.prompt("State dir", defaultStateDir, false)
	if err != nil {
		return err
	}
	mbsyncConfigPath := defaultMbsyncConfigPath

	fmt.Println()
	fmt.Println("Kopia repository:")
	fmt.Println("  [1] Create new repository")
	fmt.Println("  [2] Connect to existing repository")
	fmt.Println("  [3] Skip")
	fmt.Println()
	repoAction, err := a.prompt("Select", "1", false)
	if err != nil {
		return err
	}

	kopiaPassword, err := a.resolveKopiaPassword(existing["KOPIA_PASSWORD"], repoAction)
	if err != nil {
		return err
	}

	for _, dir := range []string{maildirPath, stateDir, filepath.Dir(kopiaConfigPath), filepath.Dir(mbsyncConfigPath)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	if repoType == "filesystem" {
		if err := os.MkdirAll(repoPath, 0o755); err != nil {
			return fmt.Errorf("create repository path: %w", err)
		}
	}

	if err := writeGeneratedMbsyncConfig(mbsyncConfigPath, imapHost, imapPort, imapUser, maildirPath); err != nil {
		return err
	}
	fmt.Printf("Wrote generated mbsync config: %s\n", mbsyncConfigPath)

	values := map[string]string{
		"KOPIA_CONFIG_PATH": kopiaConfigPath,
		"KOPIA_PASSWORD":    kopiaPassword,
		"KOPIA_REPO_TYPE":   repoType,
		"IMAP_HOST":         imapHost,
		"IMAP_PORT":         imapPort,
		"IMAP_USERNAME":     imapUser,
		"IMAP_PASSWORD":     imapPassword,
	}
	if repoType == "filesystem" {
		values["KOPIA_REPO_PATH"] = repoPath
	} else {
		values["KOPIA_S3_BUCKET"] = s3Bucket
		values["KOPIA_S3_ENDPOINT"] = s3Endpoint
		if s3Prefix != "" {
			values["KOPIA_S3_PREFIX"] = s3Prefix
		}
		if awsKeyID != "" {
			values["AWS_ACCESS_KEY_ID"] = awsKeyID
		}
		if awsSecret != "" {
			values["AWS_SECRET_ACCESS_KEY"] = awsSecret
		}
		if objectLock.Enabled {
			values["KOPIA_S3_OBJECT_LOCK_MODE"] = objectLock.RetentionMode
			values["KOPIA_S3_OBJECT_LOCK_DAYS"] = strconv.Itoa(objectLock.RetentionDays)
		}
	}
	for key, value := range existing {
		if _, ok := values[key]; !ok {
			if repoType == "s3" && key == "KOPIA_REPO_PATH" {
				continue
			}
			if repoType == "filesystem" {
				switch key {
				case "KOPIA_S3_BUCKET", "KOPIA_S3_ENDPOINT", "KOPIA_S3_PREFIX", "KOPIA_S3_OBJECT_LOCK_MODE", "KOPIA_S3_OBJECT_LOCK_DAYS":
					continue
				}
			}
			if repoType == "s3" && key == "KOPIA_S3_PREFIX" && s3PrefixCleared {
				continue
			}
			values[key] = value
		}
	}

	envPath, err := filepath.Abs(a.EnvPath)
	if err != nil {
		return err
	}
	if err := writeEnvFile(envPath, values); err != nil {
		return err
	}
	fmt.Printf("Wrote %s\n", a.EnvPath)

	if repoType == "s3" && repoAction == "1" {
		defaultObjectLock := existing["KOPIA_S3_OBJECT_LOCK_MODE"] != ""
		enableObjectLock, err := a.promptYesNo("Enable S3 Object Lock ransomware protection", defaultObjectLock)
		if err != nil {
			return err
		}
		if enableObjectLock {
			modeDefault := strings.ToUpper(strings.TrimSpace(existing["KOPIA_S3_OBJECT_LOCK_MODE"]))
			if modeDefault != "GOVERNANCE" {
				modeDefault = "COMPLIANCE"
			}
			fmt.Println("Object Lock retention mode:")
			if modeDefault == "COMPLIANCE" {
				fmt.Println("  [1] COMPLIANCE (recommended) - nobody can shorten/remove the lock")
				fmt.Println("  [2] GOVERNANCE - privileged users can override the lock")
			} else {
				fmt.Println("  [1] COMPLIANCE - nobody can shorten/remove the lock")
				fmt.Println("  [2] GOVERNANCE (current) - privileged users can override the lock")
			}
			fmt.Println()
			modePrompt := "Select retention mode"
			modeDefaultChoice := "1"
			if modeDefault == "GOVERNANCE" {
				modeDefaultChoice = "2"
			}
			modeInput, err := a.prompt(modePrompt, modeDefaultChoice, false)
			if err != nil {
				return err
			}
			switch strings.TrimSpace(strings.ToUpper(modeInput)) {
			case "1", "COMPLIANCE":
				objectLock.RetentionMode = "COMPLIANCE"
			case "2", "GOVERNANCE":
				objectLock.RetentionMode = "GOVERNANCE"
			default:
				return fmt.Errorf("invalid object lock retention mode: %s", modeInput)
			}

			retentionDaysDefault := 3
			if existingDays, err := strconv.Atoi(existing["KOPIA_S3_OBJECT_LOCK_DAYS"]); err == nil && existingDays > 0 {
				retentionDaysDefault = existingDays
			}
			retentionDaysInput, err := a.prompt("Object Lock retention days", strconv.Itoa(retentionDaysDefault), false)
			if err != nil {
				return err
			}
			retentionDays, err := strconv.Atoi(strings.TrimSpace(retentionDaysInput))
			if err != nil || retentionDays < 2 {
				return fmt.Errorf("object lock retention days must be an integer >= 2")
			}

			fmt.Println("Checking S3 Object Lock status...")
			if err := validateS3ObjectLockEnabled(s3Bucket, s3Endpoint, awsKeyID, awsSecret); err != nil {
				return fmt.Errorf("s3 object lock check failed: %w", err)
			}
			fmt.Println("S3 Object Lock is enabled on the bucket.")

			objectLock.Enabled = true
			objectLock.RetentionDays = retentionDays
		}
	}

	if repoAction == "1" || repoAction == "2" {
		env := buildEnv(map[string]string{
			"KOPIA_PASSWORD": kopiaPassword,
		})
		if repoType == "s3" {
			if awsKeyID != "" {
				env = append(env, "AWS_ACCESS_KEY_ID="+awsKeyID)
			}
			if awsSecret != "" {
				env = append(env, "AWS_SECRET_ACCESS_KEY="+awsSecret)
			}
		}
		action := "create"
		if repoAction == "2" {
			action = "connect"
		}
		command := a.buildKopiaRepoCommand(action, repoType, repoPath, s3Bucket, s3Endpoint, s3Prefix, kopiaConfigPath, kopiaPassword, objectLock)
		output, cmdErr := runCommandWithEnv(command, env)
		if cmdErr != nil {
			fmt.Printf("Kopia output:\n%s\n", output)
			return fmt.Errorf("kopia repository %s failed: %w", action, cmdErr)
		}
		fmt.Printf("Kopia repository %sed.\n", action)
		compressionCmd := a.buildKopiaCompressionPolicyCommand(kopiaConfigPath, kopiaPassword, maildirPath)
		output, cmdErr = runCommandWithEnv(compressionCmd, env)
		if cmdErr != nil {
			fmt.Printf("Kopia compression policy output:\n%s\n", output)
			return fmt.Errorf("kopia compression policy setup failed: %w", cmdErr)
		}
		fmt.Println("Set Kopia compression policy for the Maildir to zstd.")
		if action == "create" && objectLock.Enabled {
			maintenanceCmd := a.buildKopiaMaintenanceSetCommand(kopiaConfigPath, kopiaPassword, objectLock.RetentionDays)
			output, cmdErr := runCommandWithEnv(maintenanceCmd, env)
			if cmdErr != nil {
				fmt.Printf("Kopia maintenance setup output:\n%s\n", output)
				return fmt.Errorf("kopia maintenance configuration failed: %w", cmdErr)
			}
			fmt.Printf("Enabled Object Lock extension with full maintenance interval %s.\n", fullMaintenanceIntervalForObjectLock(objectLock.RetentionDays))
		}
	}

	fmt.Println()
	fmt.Println("Setup complete.")
	if objectLock.Enabled {
		fmt.Println("Object Lock note: no extra manual step. Regular backup runs will trigger Kopia maintenance to extend locks before they expire.")
	}
	fmt.Println("Run `mail-backup backup` to start backing up.")
	return nil
}

func renderMbsyncConfig(host, port, username, maildirPath string) string {
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
		"Channel mail-backup",
		"Far :remote-store:",
		"Near :local-store:",
		"Patterns *",
		"Create Near",
		"Sync Pull",
		"SyncState *",
		"Expunge Near",
		"",
	}, "\n")
}

func writeGeneratedMbsyncConfig(path, host, port, username, maildirPath string) error {
	resolvedPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(resolvedPath), 0o755); err != nil {
		return fmt.Errorf("create mbsync config dir: %w", err)
	}
	if err := os.WriteFile(resolvedPath, []byte(renderMbsyncConfig(host, port, username, maildirPath)), 0o600); err != nil {
		return fmt.Errorf("write generated mbsync config: %w", err)
	}
	return nil
}

func (a *SetupApp) buildKopiaRepoCommand(action, repoType, repoPath, s3Bucket, s3Endpoint, s3Prefix, configPath, password string, objectLock objectLockSettings) []string {
	if repoType == "filesystem" {
		absRepoPath, _ := filepath.Abs(repoPath)
		return []string{
			"kopia", "repository", action, "filesystem",
			"--path", absRepoPath,
			"--config-file", configPath,
			"--password", password,
			"--no-persist-credentials",
		}
	}
	command := []string{
		"kopia", "repository", action, "s3",
		"--bucket", s3Bucket,
		"--endpoint", s3Endpoint,
		"--config-file", configPath,
		"--password", password,
		"--no-persist-credentials",
	}
	if s3Prefix != "" {
		command = append(command, "--prefix", s3Prefix)
	}
	if action == "create" && objectLock.Enabled {
		command = append(command,
			"--retention-mode", objectLock.RetentionMode,
			"--retention-period", fmt.Sprintf("%dd", objectLock.RetentionDays),
		)
	}
	return command
}

func (a *SetupApp) buildKopiaMaintenanceSetCommand(configPath, password string, retentionDays int) []string {
	return []string{
		"kopia", "maintenance", "set",
		"--config-file", configPath,
		"--password", password,
		"--extend-object-locks", "true",
		"--full-interval", fullMaintenanceIntervalForObjectLock(retentionDays),
	}
}

func (a *SetupApp) buildKopiaCompressionPolicyCommand(configPath, password, targetPath string) []string {
	return []string{
		"kopia", "policy", "set",
		"--config-file", configPath,
		"--password", password,
		targetPath,
		"--compression", "zstd",
	}
}

func (a *SetupApp) prompt(label, defaultValue string, mask bool) (string, error) {
	if defaultValue != "" {
		display := defaultValue
		if mask {
			display = "****"
		}
		fmt.Printf("%s [%s]: ", label, display)
	} else {
		fmt.Printf("%s: ", label)
	}
	line, err := a.stdin.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return defaultValue, nil
	}
	return line, nil
}

func (a *SetupApp) promptOptional(label, defaultValue string, mask bool) (string, error) {
	value, err := a.prompt(label, defaultValue, mask)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(value) == "/" {
		return "", nil
	}
	return value, nil
}

func (a *SetupApp) resolveKopiaPassword(existingPassword, repoAction string) (string, error) {
	existingPassword = strings.TrimSpace(existingPassword)
	switch repoAction {
	case "1":
		if existingPassword != "" {
			return existingPassword, nil
		}
		generate, err := a.promptYesNo("Generate a new Kopia repository password", true)
		if err != nil {
			return "", err
		}
		if !generate {
			password, err := a.prompt("Kopia repository password", "", true)
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(password) == "" {
				return "", fmt.Errorf("setup requires a KOPIA_PASSWORD")
			}
			return password, nil
		}
		passwordBytes := make([]byte, 32)
		if _, err := rand.Read(passwordBytes); err != nil {
			return "", fmt.Errorf("generate password: %w", err)
		}
		password := base64.StdEncoding.EncodeToString(passwordBytes)
		fmt.Println(renderGeneratedKopiaPasswordNotice(password))
		return password, nil
	case "2":
		promptDefault := ""
		if existingPassword != "" {
			promptDefault = existingPassword
		}
		password, err := a.prompt("Kopia repository password for the existing repository", promptDefault, true)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(password) == "" {
			return "", fmt.Errorf("setup requires the existing KOPIA_PASSWORD to connect to the repository")
		}
		return password, nil
	case "3":
		if existingPassword != "" {
			return existingPassword, nil
		}
		password, err := a.promptOptional("Kopia repository password (optional while skipping repository setup; enter - to leave unset)", "", true)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(password), nil
	default:
		if existingPassword != "" {
			return existingPassword, nil
		}
		return "", fmt.Errorf("invalid repository action: %s", repoAction)
	}
}

func (a *SetupApp) promptYesNo(label string, defaultValue bool) (bool, error) {
	defaultText := "n"
	if defaultValue {
		defaultText = "y"
	}
	value, err := a.prompt(label+" [y/N]", defaultText, false)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid selection: %s", value)
	}
}

func parseRepoTypeChoice(choice, defaultType string) (string, error) {
	switch strings.TrimSpace(choice) {
	case "", "1":
		if defaultType == "s3" && strings.TrimSpace(choice) == "" {
			return "s3", nil
		}
		return "filesystem", nil
	case "2":
		return "s3", nil
	default:
		return "", fmt.Errorf("invalid repository type selection: %s", choice)
	}
}

func defaultRepoChoice(repoType string) string {
	if repoType == "s3" {
		return "2"
	}
	return "1"
}

func defaultIfEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func normalizeS3Prefix(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimLeft(value, "/")
	if value == "" {
		return ""
	}
	if !strings.HasSuffix(value, "/") {
		value += "/"
	}
	return value
}

func renderGeneratedKopiaPasswordNotice(password string) string {
	return strings.Join([]string{
		"",
		"============================================================",
		"IMPORTANT: GENERATED KOPIA PASSWORD",
		"",
		"If you lose this password, your backups are effectively useless.",
		"Save it somewhere safe before continuing.",
		"",
		"KOPIA_PASSWORD=" + password,
		"============================================================",
		"",
	}, "\n")
}

func writeEnvFile(path string, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteString("=")
		builder.WriteString(quoteEnvValue(values[key]))
		builder.WriteString("\n")
	}
	return os.WriteFile(path, []byte(builder.String()), 0o600)
}

func quoteEnvValue(value string) string {
	if value == "" {
		return ""
	}
	return strconvQuote(value)
}

func strconvQuote(value string) string {
	return fmt.Sprintf("%q", value)
}

func buildEnv(extra map[string]string) []string {
	env := append([]string(nil), os.Environ()...)
	for key, value := range extra {
		env = append(env, key+"="+value)
	}
	return env
}

func runCommandWithEnv(command, env []string) (string, error) {
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = env
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), err
	}
	return string(output), nil
}

func fullMaintenanceIntervalForObjectLock(retentionDays int) string {
	return fmt.Sprintf("%dh", (retentionDays-1)*24)
}

func validateS3ObjectLockEnabled(bucket, endpoint, accessKeyID, secretAccessKey string) error {
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("bucket is required")
	}
	if strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("endpoint is required")
	}
	if strings.TrimSpace(accessKeyID) == "" || strings.TrimSpace(secretAccessKey) == "" {
		return fmt.Errorf("AWS access key ID and secret access key are required for object lock validation")
	}

	endpoint = strings.TrimSpace(endpoint)
	endpointURL := endpoint
	if !strings.Contains(endpointURL, "://") {
		endpointURL = "https://" + endpointURL
	}
	endpointURL = strings.TrimRight(endpointURL, "/")

	region := inferS3Region(endpoint)
	if region == "" {
		region = "us-east-1"
	}

	req, err := http.NewRequest(http.MethodGet, endpointURL+"/"+bucket+"?object-lock=", nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	signS3RequestV4(req, region, accessKeyID, secretAccessKey, time.Now().UTC())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request bucket object lock configuration: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read bucket object lock configuration: %w", err)
	}

	return validateS3ObjectLockResponse(resp.StatusCode, body)
}

func validateS3ObjectLockResponse(statusCode int, body []byte) error {
	if statusCode != http.StatusOK {
		var s3Err s3ErrorResponse
		if err := xml.Unmarshal(body, &s3Err); err == nil && (s3Err.Code != "" || s3Err.Message != "") {
			if strings.Contains(strings.ToLower(s3Err.Message), "object lock") {
				return fmt.Errorf("this bucket does not have S3 Object Lock enabled. Create a new bucket with Object Lock enabled in Wasabi, then rerun setup and point Kopia at that bucket. Wasabi's separate Compliance setting is not enough for Kopia object-lock protection")
			}
			return fmt.Errorf("%s: %s", s3Err.Code, s3Err.Message)
		}
		return fmt.Errorf("unexpected S3 status %d", statusCode)
	}

	var cfg s3ObjectLockConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil {
		return fmt.Errorf("parse object lock configuration: %w", err)
	}
	if cfg.ObjectLockEnabled != "Enabled" {
		return fmt.Errorf("this bucket does not report S3 Object Lock as enabled. Create a new bucket with Object Lock enabled in Wasabi, then rerun setup and point Kopia at that bucket")
	}
	return nil
}

func inferS3Region(endpoint string) string {
	host := endpoint
	if strings.Contains(host, "://") {
		if parts := strings.SplitN(host, "://", 2); len(parts) == 2 {
			host = parts[1]
		}
	}
	host = strings.TrimSpace(strings.TrimRight(host, "/"))
	host = strings.Split(host, "/")[0]

	switch {
	case host == "s3.amazonaws.com":
		return "us-east-1"
	case strings.HasPrefix(host, "s3.") && strings.HasSuffix(host, ".wasabisys.com"):
		trimmed := strings.TrimPrefix(host, "s3.")
		return strings.TrimSuffix(trimmed, ".wasabisys.com")
	case strings.HasPrefix(host, "s3.") && strings.HasSuffix(host, ".amazonaws.com"):
		trimmed := strings.TrimPrefix(host, "s3.")
		return strings.TrimSuffix(trimmed, ".amazonaws.com")
	default:
		return ""
	}
}

func signS3RequestV4(req *http.Request, region, accessKeyID, secretAccessKey string, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	shortDate := now.Format("20060102")
	payloadHash := sha256HexBytes(nil)

	req.Header.Set("Host", req.URL.Host)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := req.URL.RawQuery
	canonicalHeaders := strings.Join([]string{
		"host:" + req.URL.Host,
		"x-amz-content-sha256:" + payloadHash,
		"x-amz-date:" + amzDate,
	}, "\n") + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{shortDate, region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256HexBytes([]byte(canonicalRequest)),
	}, "\n")

	signingKey := hmacSHA256([]byte("AWS4"+secretAccessKey), shortDate)
	signingKey = hmacSHA256(signingKey, region)
	signingKey = hmacSHA256(signingKey, "s3")
	signingKey = hmacSHA256(signingKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	req.Header.Set("Authorization", strings.Join([]string{
		"AWS4-HMAC-SHA256 Credential=" + accessKeyID + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}, ", "))
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}
