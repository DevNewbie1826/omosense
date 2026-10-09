package say

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type tgCall struct {
	method string
	body   *core.OMap
	file   *filePart
}

type filePart struct{ field, path string }

func (e *env) telegram(action string, a *core.OMap, bot string) int {
	call, known := tgCallFor(action, a)
	if !known {
		rejectUnknownAction(e.stderr, "telegram", action, tgActions)
		return 2
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return e.fail(err)
	}
	token, err := core.Cred(home, "telegrambot-credentials.json", bot)
	if err != nil {
		return e.fail(err)
	}
	e.token = token
	return e.postTG(token, call)
}

func tgCallFor(action string, a *core.OMap) (tgCall, bool) {
	c := tgCall{body: core.NewOMap()}
	switch action {
	case "react":
		c.method = "setMessageReaction"
		copyKeys(c.body, a, "chat_id", "message_id")
		c.body.Set("reaction", []any{emojiEntry(a)})
	case "unreact":
		c.method = "setMessageReaction"
		copyKeys(c.body, a, "chat_id", "message_id")
		c.body.Set("reaction", []any{})
	case "send":
		c.method = "sendMessage"
		copyKeys(c.body, a, "chat_id", "text")
		setReplyParams(c.body, a)
		setTruthy(c.body, a, "parse_mode", "parse_mode")
		setThread(c.body, a)
	case "edit":
		c.method = "editMessageText"
		copyKeys(c.body, a, "chat_id", "message_id", "text")
		setTruthy(c.body, a, "parse_mode", "parse_mode")
	case "draft":
		c.method = "sendMessageDraft"
		copyKeys(c.body, a, "chat_id", "draft_id", "text")
		setThread(c.body, a)
	case "typing":
		c.method = "sendChatAction"
		copyKeys(c.body, a, "chat_id")
		c.body.Set("action", "typing")
		setThread(c.body, a)
	case "topic":
		c.method = "createForumTopic"
		copyKeys(c.body, a, "chat_id", "name")
	case "topic-edit":
		c.method = "editForumTopic"
		copyKeys(c.body, a, "chat_id", "name")
		copyAs(c.body, a, "thread_id", "message_thread_id")
	case "photo", "doc":
		if action == "photo" {
			c.method = "sendPhoto"
			c.file = &filePart{field: "photo", path: strArg(a, "path")}
		} else {
			c.method = "sendDocument"
			c.file = &filePart{field: "document", path: strArg(a, "path")}
		}
		copyKeys(c.body, a, "chat_id")
		c.body.Set("caption", nullishDefault(a, "caption", ""))
		setThread(c.body, a)
	default:
		return c, false
	}
	return c, true
}

func (e *env) postTG(token string, c tgCall) int {
	req, err := newTGRequest(token, c)
	if err != nil {
		return e.fail(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return e.fail(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return e.fail(err)
	}
	v, err := core.ParseJSON(b)
	if err != nil {
		return e.fail(fmt.Errorf("telegram response: %w", err))
	}
	out, err := compactJSON(v)
	if err != nil {
		return e.fail(err)
	}
	out = e.redactJSON(out)
	fmt.Fprintln(e.stdout, string(out))
	om, _ := v.(*core.OMap)
	ok, _ := om.Get("ok")
	if ok == true {
		return 0
	}
	return 1
}

func newTGRequest(token string, c tgCall) (*http.Request, error) {
	url := tgBase() + "/bot" + token + "/" + c.method
	if c.file == nil {
		b, err := c.body.Marshal()
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}
	// Multipart parity: body fields first (strings raw, non-strings
	// JSON-encoded), then the file part (say.ts FormData order).
	f, err := os.Open(c.file.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, k := range c.body.Keys() {
		v, _ := c.body.Get(k)
		if s, is := v.(string); is {
			if err := w.WriteField(k, s); err != nil {
				return nil, err
			}
			continue
		}
		b, err := compactJSON(v)
		if err != nil {
			return nil, err
		}
		if err := w.WriteField(k, string(b)); err != nil {
			return nil, err
		}
	}
	fw, err := w.CreateFormFile(c.file.field, filepath.Base(c.file.path))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(fw, f); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func tgBase() string {
	if v := os.Getenv("OMOSENSE_TELEGRAM_API"); v != "" {
		return v
	}
	return "https://api.telegram.org"
}
