package listen

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/gorilla/websocket"
)

func TestDiscordLoggerSilentAndCloseCapture(t *testing.T) {
	testCtx(t)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	oldOut, oldErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = writer, writer
	defer func() { os.Stdout, os.Stderr = oldOut, oldErr }()
	o := &outage{clock: newClock(), sink: newSink()}
	discordActive.Lock()
	old := discordActive.outage
	discordActive.outage = o
	discordActive.Unlock()
	defer func() {
		discordActive.Lock()
		discordActive.outage = old
		discordActive.Unlock()
	}()
	if discordgo.Logger == nil {
		t.Fatal("SDK default logger remains active")
	}
	discordgo.Logger(discordgo.LogInformational, 2, "Closing and reconnecting in response to Op7")
	if o.code != 1005 {
		t.Fatal("Op7 was not captured", o.code)
	}
	discordgo.Logger(discordgo.LogInformational, 2, "sending identify packet to gateway in response to Op9")
	if o.code != 1005 {
		t.Fatal("Op9 changed close capture", o.code)
	}
	discordgo.Logger(discordgo.LogWarning, 2, "error reading from gateway %s websocket, %s", "fake", fmt.Errorf("wrapped: %w", &websocket.CloseError{Code: 4000}))
	if o.code != 4000 {
		t.Fatal("wrapped close error was not captured", o.code)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil || len(output) != 0 {
		t.Fatalf("SDK emitted stdout/stderr: %q %v", output, err)
	}
}
