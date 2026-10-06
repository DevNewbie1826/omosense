package listen

import (
	"context"
	"encoding/json"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type discordAuthor struct {
	ID         string  `json:"id"`
	Username   *string `json:"username"`
	GlobalName *string `json:"global_name"`
	Bot        bool    `json:"bot"`
}

type discordMessage struct {
	ID          string            `json:"id"`
	Guild       *string           `json:"guild_id"`
	Channel     string            `json:"channel_id"`
	Author      *discordAuthor    `json:"author"`
	Content     *string           `json:"content"`
	Snapshots   []json.RawMessage `json:"message_snapshots"`
	Reply       *discordMessage   `json:"referenced_message"`
	Flags       int               `json:"flags"`
	Attachments []struct {
		Name string  `json:"filename"`
		URL  string  `json:"url"`
		Type *string `json:"content_type"`
	} `json:"attachments"`
}

func (s src) dcHandle(ctx context.Context, sink core.Sink, bot string, raw json.RawMessage) error {
	var d discordMessage
	if err := json.Unmarshal(raw, &d); err != nil {
		return err
	}
	if d.Author != nil && d.Author.Bot {
		return nil
	}
	ev := map[string]any{"platform": "discord", "bot": bot, "kind": "message", "guild_id": d.Guild, "channel_id": d.Channel, "message_id": d.ID, "text": textOr(d.Content, nil), "forwarded": len(d.Snapshots) > 0, "reply_to": nil}
	if d.Author != nil {
		ev["from_id"] = d.Author.ID
		if d.Author.GlobalName != nil || d.Author.Username != nil {
			ev["from"] = textOr(d.Author.GlobalName, d.Author.Username)
		}
		ev["role"] = s.role("discord", d.Author.ID)
	} else {
		ev["role"] = "other"
	}
	if d.Reply != nil {
		reply := map[string]any{"message_id": d.Reply.ID}
		if d.Reply.Content != nil {
			reply["text"] = *d.Reply.Content
		}
		if d.Reply.Author != nil && d.Reply.Author.Username != nil {
			reply["from"] = *d.Reply.Author.Username
		}
		ev["reply_to"] = reply
	}
	attachments := []map[string]any{}
	for _, a := range d.Attachments {
		item := map[string]any{"name": a.Name, "url": a.URL}
		if a.Type != nil {
			item["type"] = *a.Type
		}
		attachments = append(attachments, item)
	}
	ev["attachments"] = attachments
	if d.Flags&8192 != 0 && len(d.Attachments) != 0 {
		text, err := transcribe(ctx, d.Attachments[0].URL)
		if err != nil {
			ev["transcribe_error"] = err.Error()
		} else {
			ev["text"] = text
			ev["transcribed"] = true
		}
	}
	sink.Emit("EVENT", ev)
	return nil
}
