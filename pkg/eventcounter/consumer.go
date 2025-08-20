package eventcounter

import (
	"context"
	"math/rand"
	"time"
)

type Consumer interface {
	Created(ctx context.Context, uid string) error
	Updated(ctx context.Context, uid string) error
	Deleted(ctx context.Context, uid string) error
}

type ConsumerWrapper struct {
	consumer Consumer
}

func (c *ConsumerWrapper) randomSleep() {
	time.Sleep(time.Second * time.Duration(rand.Intn(30)))
}

func (c *ConsumerWrapper) Created(ctx context.Context, uid string) error {
	c.randomSleep()
	return c.Created(ctx, uid)
}

func (c *ConsumerWrapper) Updated(ctx context.Context, uid string) error {
	c.randomSleep()
	return c.Updated(ctx, uid)
}

func (c *ConsumerWrapper) Deleted(ctx context.Context, uid string) error {
	c.randomSleep()
	return c.Deleted(ctx, uid)
}
