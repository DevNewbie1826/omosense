package say

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type dcCall struct {
	method string
	path   string
	body   *core.OMap // nil means no body and no Content-Type
	file   string     // non-empty sends multipart payload_json + files[0]
}

func (e *env) discord(action string, a *core.OMap, cfgBot string) int {
	call, known := dcCallFor(action, a)
	if !known {
		rejectUnknownAction(e.stderr, "discord", action, dcActions)
		return 2
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return e.fail(err)
	}
	token, err := core.Cred(home, "discordbot-credentials.json", cfgBot)
	if err != nil {
		return e.fail(err)
	}
	e.token = token
	return e.doDC(token, call)
}

func dcCallFor(action string, a *core.OMap) (dcCall, bool) {
	var c dcCall
	emoji := encodeURIComponent(emojiStr(a))
	switch action {
	case "react":
		c.method, c.path = http.MethodPut, messagePath(a, "/reactions/"+emoji+"/@me")
	case "unreact":
		c.method, c.path = http.MethodDelete, messagePath(a, "/reactions/"+emoji+"/@me")
	case "send":
		c.method, c.path = http.MethodPost, "/channels/"+jsStr(a, "channel_id")+"/messages"
		c.body = core.NewOMap()
		copyDiscordContent(c.body, a)
		setMessageRef(c.body, a)
	case "edit":
		c.method, c.path = http.MethodPatch, messagePath(a, "")
		c.body = core.NewOMap()
		copyDiscordContent(c.body, a)
	case "typing":
		c.method, c.path = http.MethodPost, "/channels/"+jsStr(a, "channel_id")+"/typing"
	case "thread":
		c.body = core.NewOMap()
		copyKeys(c.body, a, "name")
		if v, ok := a.Get("message_id"); ok && truthy(v) {
			c.method, c.path = http.MethodPost, "/channels/"+jsStr(a, "channel_id")+"/messages/"+jsStr(a, "message_id")+"/threads"
		} else {
			c.method, c.path = http.MethodPost, "/channels/"+jsStr(a, "channel_id")+"/threads"
			c.body.Set("type", json.Number("11"))
		}
	case "thread-edit":
		c.method, c.path = http.MethodPatch, "/channels/"+jsStr(a, "thread_id")
		c.body = core.NewOMap()
		setTruthy(c.body, a, "name", "name")
		if v, ok := a.Get("archived"); ok {
			c.body.Set("archived", v)
		}
	case "file":
		c.method, c.path = http.MethodPost, "/channels/"+jsStr(a, "channel_id")+"/messages"
		c.body = core.NewOMap()
		c.body.Set("content", discordFileContent(a))
		c.file = strArg(a, "path")
	default:
		return c, false
	}
	return c, true
}

// copyDiscordContent copies a present "text" key onto body "content", even
// when the value is null. Only when "text" is absent does a present
// "content" key get copied. text wins when both keys are set.
func copyDiscordContent(dst, src *core.OMap) {
	if _, ok := src.Get("text"); ok {
		copyAs(dst, src, "text", "content")
		return
	}
	if _, ok := src.Get("content"); ok {
		copyAs(dst, src, "content", "content")
	}
}

// discordFileContent is text ?? content ?? "" (nullish). A null text falls
// through to content, unlike copyDiscordContent.
func discordFileContent(src *core.OMap) any {
	if v, ok := src.Get("text"); ok && v != nil {
		return v
	}
	if v, ok := src.Get("content"); ok && v != nil {
		return v
	}
	return ""
}

func messagePath(a *core.OMap, suffix string) string {
	return "/channels/" + jsStr(a, "channel_id") + "/messages/" + jsStr(a, "message_id") + suffix
}

func setMessageRef(dst, src *core.OMap) {
	v, ok := src.Get("reply_to")
	if !ok || !truthy(v) {
		return
	}
	ref := core.NewOMap()
	ref.Set("message_id", v)
	dst.Set("message_reference", ref)
}

func (e *env) doDC(token string, c dcCall) int {
	req, err := newDCRequest(token, c)
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
	out := core.NewOMap()
	out.Set("status", json.Number(strconv.Itoa(resp.StatusCode)))
	if len(b) > 0 {
		v, err := core.ParseJSON(b)
		if err != nil {
			return e.fail(fmt.Errorf("discord response: %w", err))
		}
		if body, ok := v.(*core.OMap); ok {
			for _, k := range body.Keys() {
				val, _ := body.Get(k)
				out.Set(k, val)
			}
		}
	}
	o, err := out.Marshal()
	if err != nil {
		return e.fail(err)
	}
	o = e.redactJSON(o)
	fmt.Fprintln(e.stdout, string(o))
	if resp.StatusCode >= 400 {
		return 1
	}
	return 0
}

func newDCRequest(token string, c dcCall) (*http.Request, error) {
	var body io.Reader
	contentType := ""
	if c.file != "" {
		f, err := os.Open(c.file)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		var buf bytes.Buffer
		w := multipart.NewWriter(&buf)
		pj, err := c.body.Marshal()
		if err != nil {
			return nil, err
		}
		if err := w.WriteField("payload_json", string(pj)); err != nil {
			return nil, err
		}
		fw, err := w.CreateFormFile("files[0]", filepath.Base(c.file))
		if err != nil {
			return nil, err
		}
		if _, err := io.Copy(fw, f); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
		body, contentType = &buf, w.FormDataContentType()
	} else if c.body != nil {
		pj, err := c.body.Marshal()
		if err != nil {
			return nil, err
		}
		body, contentType = bytes.NewReader(pj), "application/json"
	}
	req, err := http.NewRequest(c.method, dcBase()+c.path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bot "+token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nil
}

func dcBase() string {
	if v := os.Getenv("OMOSENSE_DISCORD_API"); v != "" {
		return v
	}
	return "https://discord.com/api/v10"
}
