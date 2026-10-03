package listen

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type telegramUser struct {
	ID        *int64  `json:"id"`
	Username  *string `json:"username"`
	FirstName *string `json:"first_name"`
}
type telegramMessage struct {
	ID   int64 `json:"message_id"`
	Chat struct {
		ID   int64  `json:"id"`
		Type string `json:"type"`
	} `json:"chat"`
	Thread  *int64          `json:"message_thread_id"`
	From    *telegramUser   `json:"from"`
	Text    *string         `json:"text"`
	Caption *string         `json:"caption"`
	Forward json.RawMessage `json:"forward_origin"`
	Quote   *struct {
		Text *string `json:"text"`
	} `json:"quote"`
	Reply    *telegramMessage `json:"reply_to_message"`
	Photo    json.RawMessage  `json:"photo"`
	Document json.RawMessage  `json:"document"`
	Video    json.RawMessage  `json:"video"`
	Voice    *struct {
		FileID string `json:"file_id"`
	} `json:"voice"`
	Audio     json.RawMessage `json:"audio"`
	VideoNote *struct {
		FileID string `json:"file_id"`
	} `json:"video_note"`
}
type telegramUpdate struct {
	ID      int64            `json:"update_id"`
	Message *telegramMessage `json:"message"`
	Edited  *telegramMessage `json:"edited_message"`
	Stopped json.RawMessage  `json:"stopped_message_generation"`
}

func textOr(first, second *string) string {
	if first != nil {
		return *first
	}
	if second != nil {
		return *second
	}
	return ""
}
func present(v json.RawMessage) bool {
	return len(v) > 0 && string(v) != "null" && string(v) != "false"
}

func (s src) tgHandle(ctx context.Context, sink core.Sink, bot string, api telegramAPI, u telegramUpdate) error {
	m := u.Message
	kind := "message"
	if u.Edited != nil {
		kind = "edited"
		if m == nil {
			m = u.Edited
		}
	}
	if m == nil {
		if present(u.Stopped) {
			sink.Emit("EVENT", map[string]any{"platform": "telegram", "bot": bot, "kind": "generation_stopped", "data": u.Stopped})
		}
		return nil
	}
	ev := map[string]any{"platform": "telegram", "bot": bot, "kind": kind, "chat_id": m.Chat.ID, "chat_type": m.Chat.Type, "thread_id": m.Thread, "message_id": m.ID, "role": "other", "text": textOr(m.Text, m.Caption), "forwarded": present(m.Forward), "quote": nil, "reply_to": nil, "attachments": []string{}}
	if m.From != nil {
		if m.From.ID != nil {
			ev["from_id"] = *m.From.ID
			ev["role"] = s.role("telegram", strconv.FormatInt(*m.From.ID, 10))
		}
		if m.From.Username != nil || m.From.FirstName != nil {
			ev["from"] = textOr(m.From.Username, m.From.FirstName)
		}
	}
	if m.Quote != nil {
		ev["quote"] = m.Quote.Text
	}
	if m.Reply != nil {
		reply := map[string]any{"message_id": m.Reply.ID, "text": textOr(m.Reply.Text, m.Reply.Caption)}
		if m.Reply.From != nil && m.Reply.From.Username != nil {
			reply["from"] = *m.Reply.From.Username
		}
		ev["reply_to"] = reply
	}
	attachments := []string{}
	for _, a := range []struct {
		name string
		has  bool
	}{{"photo", present(m.Photo)}, {"document", present(m.Document)}, {"video", present(m.Video)}, {"voice", m.Voice != nil}, {"audio", present(m.Audio)}} {
		if a.has {
			attachments = append(attachments, a.name)
		}
	}
	ev["attachments"] = attachments
	voice := m.Voice
	if voice == nil {
		voice = m.VideoNote
	}
	if voice != nil {
		text, err := telegramVoice(ctx, api, voice.FileID)
		if err != nil {
			ev["transcribe_error"] = redact(err, api.token)
		} else {
			ev["text"] = text
			ev["transcribed"] = true
		}
	}
	sink.Emit("EVENT", ev)
	return nil
}
