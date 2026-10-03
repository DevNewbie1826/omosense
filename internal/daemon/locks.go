package daemon

import (
	"sync"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type sharedLock struct {
	refs    int
	release func()
}

// lockPool serializes TryAcquire and refcounts the shared telegram/discord
// lock. It is per daemon, never process-global.
type lockPool struct {
	mu   sync.Mutex
	held map[string]*sharedLock
}

func (p *lockPool) acquire(ctx *core.Ctx, name, legacy string) (func(), string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := ctx.State + "\x00" + name
	held := p.held[key]
	if held == nil {
		release, blocked, metadata, err := ctx.TryAcquire(name, legacy)
		if err != nil {
			return nil, "", err
		}
		if blocked != "" {
			b, err := metadata.Marshal()
			if err != nil {
				return nil, "", err
			}
			return nil, blocked + " " + string(b), nil
		}
		held = &sharedLock{release: release}
		p.held[key] = held
	}
	held.refs++
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		held.refs--
		if held.refs == 0 {
			held.release()
			delete(p.held, key)
		}
	}, "", nil
}
