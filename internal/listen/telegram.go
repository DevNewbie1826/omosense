package listen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type telegramAPI struct {
	base, token string
	client      *http.Client
}

type telegramResponse struct {
	OK          bool            `json:"ok"`
	Description string          `json:"description"`
	Result      json.RawMessage `json:"result"`
}

func (a telegramAPI) call(ctx context.Context, method string, body any) (telegramResponse, error) {
	var result telegramResponse
	b, err := json.Marshal(body)
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", a.base+"/bot"+a.token+"/"+method, bytes.NewReader(b))
	if err != nil {
		return result, fmt.Errorf("telegram request: %s", redact(err, a.token))
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(req)
	if err != nil {
		return result, fmt.Errorf("%s", redact(err, a.token))
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return result, fmt.Errorf("decode telegram: %w", err)
	}
	result.Description = strings.ReplaceAll(result.Description, a.token, "[redacted]")
	return result, nil
}

// net/url errors include the credential-bearing request path. Never log it.
func redact(err error, token string) string {
	if e, ok := err.(*url.Error); ok {
		err = e.Err
	}
	return strings.ReplaceAll(err.Error(), token, "[redacted]")
}

func (s src) telegram(ctx context.Context, sink core.Sink) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(s.cfg.Profile.Telegram))
	for _, bot := range s.cfg.Profile.Telegram {
		token, err := core.Cred(os.Getenv("HOME"), "telegrambot-credentials.json", bot)
		if err != nil {
			cancel()
			wg.Wait()
			return err
		}
		base := strings.TrimRight(os.Getenv("OMOSENSE_TELEGRAM_API"), "/")
		if base == "" {
			base = "https://api.telegram.org"
		}
		api := telegramAPI{base, token, &http.Client{Timeout: 70 * time.Second}}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.poll(ctx, sink, bot, api); err != nil {
				errs <- err
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func (s src) poll(ctx context.Context, sink core.Sink, bot string, api telegramAPI) error {
	path := filepath.Join(s.cfg.State, "tg-offset-"+bot)
	offset := int64(0)
	b, err := os.ReadFile(path)
	if err == nil {
		offset, err = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil {
			return fmt.Errorf("read telegram offset: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read telegram offset: %w", err)
	}
	var handlers sync.WaitGroup
	defer handlers.Wait()
	fails := 0
	for ctx.Err() == nil {
		r, err := api.call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": 50, "allowed_updates": []string{"message", "edited_message", "stopped_message_generation"}})
		if ctx.Err() != nil {
			break
		}
		var updates []telegramUpdate
		message := ""
		switch {
		case err != nil:
			message = "poll " + err.Error()
		case !r.OK:
			message = "getUpdates error " + r.Description
		default:
			if err := json.Unmarshal(r.Result, &updates); err != nil {
				message = "poll " + err.Error()
			}
		}
		if message != "" {
			fails++
			if fails == 60 {
				sink.Log(fmt.Sprintf("telegram %s failing 60x in a row: %s", bot, message))
			}
			if err := s.sleep(ctx, 5*time.Second); err != nil {
				break
			}
			continue
		}
		if fails >= 60 {
			sink.Log(fmt.Sprintf("telegram %s recovered after %d failures", bot, fails))
		}
		fails = 0
		for _, u := range updates {
			offset = u.ID + 1
			if err := os.WriteFile(path, []byte(strconv.FormatInt(offset, 10)), 0644); err != nil {
				return fmt.Errorf("write telegram offset: %w", err)
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				if err := s.tgHandle(ctx, sink, bot, api, u); err != nil && ctx.Err() == nil {
					sink.Log("tg handle " + redact(err, api.token))
				}
			}()
		}
	}
	return nil
}

func (s src) role(platform, id string) string {
	section, _ := s.cfg.Cfg.Raw.Get(platform)
	m, ok := section.(*core.OMap)
	if !ok {
		return "other"
	}
	if owner, ok := m.Get("owner"); ok && fmt.Sprint(owner) == id {
		return "owner"
	}
	if wife, ok := m.Get("wife"); ok && wife != nil && fmt.Sprint(wife) == id {
		return "wife"
	}
	return "other"
}
