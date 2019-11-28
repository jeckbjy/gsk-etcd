package etcd3

import (
	"context"

	"github.com/jeckbjy/gsk/store"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

func NewStore() store.Store {
	s := &_Store{}
	return s
}

type _Store struct {
	client *clientv3.Client
}

func (s *_Store) Name() string {
	return "etcd3"
}

func (s *_Store) List(ctx context.Context, key string, opts ...store.Option) ([]*store.KV, error) {
	eopts := make([]clientv3.OpOption, 0, len(opts) + 1)
	keyOnly := false
	if len(opts) > 0 {
		o := store.Options{}
		o.Build(opts...)
		if o.KeyOnly {
			eopts = append(eopts, clientv3.WithKeysOnly())
			keyOnly = o.KeyOnly
		}
		if o.Revision != 0 {
			eopts = append(eopts, clientv3.WithRev(o.Revision))
		}
	}
	eopts = append(eopts, clientv3.WithPrefix())

	result, err := s.client.KV.Get(ctx, key, eopts...)
	if err != nil {
		return nil, err
	}

	if len(result.Kvs) == 0 {
		return nil, store.ErrNotFound
	}

	results := make([]*store.KV, 0, len(result.Kvs))
	for _, kv := range result.Kvs {
		results = append(results, toKV(kv,keyOnly))
	}

	return nil, nil
}

func (s *_Store) Get(ctx context.Context, key string, opts ...store.Option) (*store.KV, error) {
	eopts := make([]clientv3.OpOption, 0)
	keyOnly := false
	if len(opts) > 0 {
		o := store.Options{}
		o.Build(opts...)
		if o.KeyOnly {
			eopts = append(eopts, clientv3.WithKeysOnly())
			keyOnly = o.KeyOnly
		}
		if o.Revision != 0 {
			eopts = append(eopts, clientv3.WithRev(o.Revision))
		}
	}

	result, err := s.client.KV.Get(ctx, key, eopts...)
	if err != nil {
		return nil, err
	}

	if len(result.Kvs) == 0 {
		return nil, store.ErrNotFound
	}

	return toKV(result.Kvs[0], keyOnly), nil
}

func (s *_Store) Put(ctx context.Context, key string, value []byte) error {
	_, err := s.client.KV.Put(ctx,key, string(value))
	return err
}

func (s *_Store) Delete(ctx context.Context, key string, opts ...store.Option) error {
	prefix := false
	if len(opts) > 0 {
		o := store.Options{}
		o.Build(opts...)
		prefix = o.Prefix
	}

	if prefix {
		_, err := s.client.KV.Delete(ctx, key, clientv3.WithPrefix())
		return err
	} else {
		_, err := s.client.KV.Delete(ctx, key)
		return err
	}
}

func (s *_Store) Exists(ctx context.Context, key string) (bool, error) {
	result, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return false, err
	}

	return len(result.Kvs) > 0, nil
}

func (s *_Store) Watch(ctx context.Context, key string, cb store.Callback, opts ...store.Option) error {
	go func() {
		wc := clientv3.NewWatcher(s.client)
		defer wc.Close()
		var wch clientv3.WatchChan
		keyOnly := false
		if len(opts) > 0 {
			o := store.Options{}
			o.Build(opts...)
			eo := make([]clientv3.OpOption, 0,len(opts))
			if o.Prefix {
				eo = append(eo, clientv3.WithPrefix())
			}
			if o.KeyOnly {
				keyOnly = true
				eo = append(eo, clientv3.WithKeysOnly())
			}
			if o.Revision > 0 {
				eo = append(eo, clientv3.WithRev(o.Revision))
			}
			wch = wc.Watch(ctx, key, eo...)
		} else {
			wch = wc.Watch(ctx, key)
		}

		for {
			select {
			case rsp, ok := <-wch:
				if !ok || rsp.Canceled {
					break
				}
				for _, ev := range rsp.Events {
					cb(&store.Event{
						Type:store.EventType(ev.Type),
						Data:toKV(ev.Kv, keyOnly),
						Prev:toKV(ev.PrevKv, keyOnly),
					})
				}
			}
		}
	}()
	return nil
}

func (s *_Store) Close() error {
	return s.client.Close()
}

func toKV(kv *mvccpb.KeyValue, keyOnly bool) *store.KV {
	if keyOnly {
		return &store.KV{Key:string(kv.Key)}
	} else {
		return &store.KV{Key:string(kv.Key), Value:kv.Value, Version:kv.Version, CreateRevision:kv.CreateRevision, ModifyRevision:kv.ModRevision}
	}
}