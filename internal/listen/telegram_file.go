package listen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var telegramExtension = regexp.MustCompile(`^[a-z0-9]{1,10}$`)

func telegramBasename(bot string, m *telegramMessage, kind string, file telegramFile) string {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(file.FileName)), ".")
	if !telegramExtension.MatchString(ext) {
		ext = map[string]string{
			"image/jpeg": "jpg", "image/png": "png", "image/gif": "gif",
			"image/webp": "webp", "image/heic": "heic", "video/mp4": "mp4",
			"video/quicktime": "mov", "video/webm": "webm", "audio/mpeg": "mp3",
			"audio/mp4": "m4a", "audio/ogg": "ogg", "audio/wav": "wav",
			"audio/x-wav": "wav", "audio/flac": "flac", "application/pdf": "pdf",
			"application/zip": "zip", "text/plain": "txt",
		}[file.MIMEType]
	}
	if ext == "" {
		ext = map[string]string{"photo": "jpg", "video": "mp4", "audio": "mp3", "document": "bin"}[kind]
	}
	return fmt.Sprintf("tg-%s-%d-%d.%s", bot, m.Chat.ID, m.ID, ext)
}

func (s src) telegramDownload(ctx context.Context, api telegramAPI, id, name string) (string, error) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, api.base+"/file/bot"+api.token+"/"+file.Path, nil)
	if err != nil {
		return "", err
	}
	response, err := api.client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("download HTTP %d", response.StatusCode)
	}
	dir, err := filepath.Abs(filepath.Join(s.cfg.State, "inbox"))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(dir, ".tg-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	_, copyErr := io.Copy(tmp, response.Body)
	closeErr := tmp.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	path := filepath.Join(dir, name)
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	return path, nil
}
