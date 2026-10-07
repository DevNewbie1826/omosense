package host

import (
	"sync"

	"github.com/DevNewbie1826/omosense/internal/core"
)

type sharedLock struct {
	refs    int
	release func()
}

// lockPool serializes TryAcquire and refcounts a lock two sources share:
// telegram and discord both take the listen lock. It is per host, never
// process-global.
type lockPool struct {
	mu   sync.Mutex
	held map[string]*sharedLock
}

func newLockPool() *lockPool {
	return &lockPool{held: make(map[string]*sharedLock)}
}

// acquire takes name (or reports the blocking holder as "<name> <json>").
// The returned release drops one reference and removes the lock file only
// when the last holder releases it.
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
