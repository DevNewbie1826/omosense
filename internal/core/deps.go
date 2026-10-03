//go:build tools

// Blank imports pin the module's allowed dependencies so go mod tidy keeps
// them while the packages that use them are still stubs. This file is
// excluded from normal builds by the tools build tag.
package core

import (
	_ "github.com/bwmarrin/discordgo"
	_ "github.com/gorilla/websocket"
	_ "gopkg.in/yaml.v3"
)
