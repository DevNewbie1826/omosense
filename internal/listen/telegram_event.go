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
type telegramPhotoSize struct {
	FileID       string `json:"file_id"`
	FileUniqueID string `json:"file_unique_id"`
	Width        int64  `json:"width"`
	Height       int64  `json:"height"`
	FileSize     int64  `json:"file_size"`
}
type telegramFile struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MIMEType string `json:"mime_type"`
	FileSize int64  `json:"file_size"`
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
	Reply     *telegramMessage    `json:"reply_to_message"`
	Photo     []telegramPhotoSize `json:"photo"`
	Document  *telegramFile       `json:"document"`
	Video     *telegramFile       `json:"video"`
	Voice     *telegramFile       `json:"voice"`
	Audio     *telegramFile       `json:"audio"`
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
	ev := map[string]any{"platform": "telegram", "bot": bot, "kind": kind, "chat_id": m.Chat.ID, "chat_type": m.Chat.Type, "thread_id": m.Thread, "message_id": m.ID, "role": "other", "text": textOr(m.Text, m.Caption), "forwarded": present(m.Forward), "quote": nil, "reply_to": nil}
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
	var photo *telegramFile
	if len(m.Photo) > 0 {
		largest := m.Photo[0]
		for _, p := range m.Photo[1:] {
			area, best := p.Width*p.Height, largest.Width*largest.Height
			if area > best || (area == best && p.FileSize >= largest.FileSize) {
				largest = p
			}
		}
		photo = &telegramFile{FileID: largest.FileID, MIMEType: "image/jpeg", FileSize: largest.FileSize}
	}
	attachments := []map[string]any{}
	for _, a := range []struct {
		kind string
		file *telegramFile
	}{{"photo", photo}, {"document", m.Document}, {"video", m.Video}, {"voice", m.Voice}, {"audio", m.Audio}} {
		if a.file == nil {
			continue
		}
		item := map[string]any{"kind": a.kind, "file_id": a.file.FileID}
		if a.file.MIMEType != "" {
			item["type"] = a.file.MIMEType
		}
		if a.file.FileSize != 0 {
			item["size"] = a.file.FileSize
		}
		if a.kind != "voice" {
			name := telegramBasename(bot, m, a.kind, *a.file)
			item["name"] = name
			if a.file.FileName != "" {
				item["name"] = a.file.FileName
			}
			path, err := s.telegramDownload(ctx, api, a.file.FileID, name)
			if err != nil {
				item["error"] = redact(err, api.token)
			} else {
				item["path"] = path
			}
		}
		attachments = append(attachments, item)
	}
	ev["attachments"] = attachments
	voiceID := ""
	hasVoice := m.Voice != nil || m.VideoNote != nil
	if m.Voice != nil {
		voiceID = m.Voice.FileID
	} else if m.VideoNote != nil {
		voiceID = m.VideoNote.FileID
	}
	if hasVoice {
		text, err := telegramVoice(ctx, api, voiceID)
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
