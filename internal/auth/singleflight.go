package auth

import "sync"

// Minimal golang.org/x/sync/singleflight substitute: stdlib only, one key type.
type singleflight struct {
	mu    sync.Mutex
	calls map[[32]byte]*call
}

type call struct {
	wg  sync.WaitGroup
	key *Key
	err error
}

func (g *singleflight) Do(h [32]byte, fn func() (*Key, error)) (*Key, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = map[[32]byte]*call{}
	}
	if c, ok := g.calls[h]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.key, c.err
	}
	c := &call{}
	c.wg.Add(1)
	g.calls[h] = c
	g.mu.Unlock()

	c.key, c.err = fn()

	g.mu.Lock()
	delete(g.calls, h)
	g.mu.Unlock()
	c.wg.Done()
	return c.key, c.err
}
