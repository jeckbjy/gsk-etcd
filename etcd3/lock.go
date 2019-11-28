package etcd3

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/jeckbjy/gsk/sync/lock"
)

func NewLocking() lock.Locking {
	l := &_Locking{}
	return l
}

type _Locking struct {
	client *clientv3.Client
}

func (*_Locking) Name() string {
	return "etcd3"
}

func (l *_Locking) Acquire(key string, opts *lock.Options) (lock.Locker, error) {
	sess, err := concurrency.NewSession(l.client, concurrency.WithTTL(int(opts.TTL / time.Second)))
	if err != nil {
		return nil, err
	}
	mux := concurrency.NewMutex(sess, key)
	return &_Locker{mux: mux, timeout:opts.Timeout}, nil
}

type _Locker struct {
	mux *concurrency.Mutex
	timeout time.Duration
}

func (l *_Locker) Lock() error {
	switch l.timeout {
	case lock.TimeoutMax:
		return l.mux.Lock(context.Background())
	case 0:
		// 新版本才有TryLock方法,等更新
		ctx, _ := context.WithTimeout(context.Background(), time.Nanosecond)
		return l.mux.Lock(ctx)
	default:
		ctx, _ := context.WithTimeout(context.Background(), l.timeout)
		return l.mux.Lock(ctx)
	}
}

func (l *_Locker) Unlock() {
	_ = l.mux.Unlock(context.Background())
}
