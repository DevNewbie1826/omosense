package listen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func transcribe(ctx context.Context, url string) (text string, err error) {
	// A unique directory prevents concurrent voice messages from overwriting
	// each other's ffmpeg/Whisper files. All artifacts are removed on every exit.
	dir, err := os.MkdirTemp("", "omosense-voice-")
	if err != nil {
		return "", fmt.Errorf("voice temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(dir); err == nil && cleanupErr != nil {
			err = fmt.Errorf("voice cleanup: %w", cleanupErr)
		}
	}()
	base := filepath.Join(dir, "omosense-voice")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("voice request: %w", err)
	}
	response, err := (&http.Client{Timeout: 70 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	file, err := os.Create(base + ".ogg")
	if err != nil {
		return "", fmt.Errorf("voice download file: %w", err)
	}
	_, copyErr := io.Copy(file, response.Body)
	closeErr := file.Close()
	if copyErr != nil {
		return "", fmt.Errorf("voice download: %w", copyErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("voice download close: %w", closeErr)
	}
	for _, command := range [][]string{
		{"ffmpeg", "-loglevel", "error", "-y", "-i", base + ".ogg", "-ar", "16000", "-ac", "1", base + ".wav"},
		{"mlx_whisper", base + ".wav", "--model", "mlx-community/whisper-large-v3-turbo", "--output-format", "txt", "--output-dir", dir, "--verbose", "False"},
	} {
		if err := exec.CommandContext(ctx, command[0], command[1:]...).Run(); err != nil {
			return "", fmt.Errorf("%s: %w", command[0], err)
		}
	}
	b, err := os.ReadFile(base + ".txt")
	if err != nil {
		return "", fmt.Errorf("voice transcript: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

func telegramVoice(ctx context.Context, api telegramAPI, id string) (string, error) {
	r, err := api.call(ctx, "getFile", map[string]string{"file_id": id})
	if err != nil {
		return "", err
	}
	if !r.OK {
		return "", fmt.Errorf("getFile error %s", r.Description)
	}
	var file struct {
		Path string `json:"file_path"`
	}
	if err := json.Unmarshal(r.Result, &file); err != nil {
		return "", fmt.Errorf("getFile response: %w", err)
	}
	return transcribe(ctx, api.base+"/file/bot"+api.token+"/"+file.Path)
}
