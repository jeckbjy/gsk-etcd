package etcd3

import (
	"context"
	"sync"

	"github.com/jeckbjy/gsk/util/errorx"

	"github.com/coreos/etcd/clientv3"
	"github.com/jeckbjy/gsk/registry"
)

func NewRegistry() registry.Registry {
	r := &_Registry{}
	return r
}

type _CancelFunc = func()

type _Registry struct {
	sync.RWMutex
	opt     registry.Options
	client  *clientv3.Client
	leases  map[string]clientv3.LeaseID
	watcher []_CancelFunc
}

func (r *_Registry) Name() string {
	return "etcd3"
}

func (r *_Registry) Register(service *registry.Service) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.opt.Timeout)
	defer cancel()

	r.Lock()
	leaseID, ok := r.leases[service.Id]
	if !ok {
		// 没注册过,新建
		ttl := r.opt.TTL.Seconds()
		rsp, err := r.client.Grant(ctx, int64(ttl))
		if err != nil {
			return err
		}
		leaseID = rsp.ID
		r.leases[service.Id] = leaseID
	}
	r.Unlock()

	var err error
	if leaseID != 0 {
		_, err = r.client.Put(ctx, r.getKey(service.Id), service.Marshal(), clientv3.WithLease(leaseID))
	} else {
		_, err = r.client.Put(ctx, r.getKey(service.Id), service.Marshal())
	}

	if err != nil {
		return err
	}

	// TODO:keepalive
	// 使用官方的KeepAlive会多一个协程,而且如果网络中断超时很久,貌似没有办法自动恢复,虽然并不容易出现?
	// 使用KeepAliveOnce,自己维护定时器？
	return nil
}

func (r *_Registry) Unregister(service string) error {
	var err error
	r.Lock()
	if _, ok := r.leases[service]; ok {
		ctx, cancel := context.WithTimeout(context.Background(), r.opt.Timeout)
		defer cancel()
		_, err = r.client.Delete(ctx, r.getKey(service))
		delete(r.leases, service)
	}
	r.Unlock()
	return err
}

func (r *_Registry) Query(name string, filters map[string]string) ([]*registry.Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.opt.Timeout)
	defer cancel()

	rsp, err := r.client.Get(ctx, r.getKey(name), clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return nil, errorx.ErrNotFound
	}

	results := make([]*registry.Service, 0, len(rsp.Kvs))
	for _, kv := range rsp.Kvs {
		srv, err := registry.Unmarshal(string(kv.Value))
		if err != nil {
			continue
		}
		results = append(results, srv)
	}

	if len(results) == 0 {
		return nil, errorx.ErrNotFound
	}

	return results, nil
}

func (r *_Registry) Watch(services []string, cb registry.Callback) error {
	if len(services) == 0 {
		// watch all
		go r.doWatch("", cb)
	} else {
		for _, key := range services {
			go r.doWatch(key, cb)
		}
	}
	return nil
}

func (r *_Registry) doWatch(key string, cb registry.Callback) {
	watcher := clientv3.NewWatcher(r.client)
	defer watcher.Close()
	ctx, cancel := context.WithCancel(context.Background())
	wch := watcher.Watch(ctx, r.getKey(key), clientv3.WithPrefix())
	r.Lock()
	r.watcher = append(r.watcher, cancel)
	r.Unlock()
	for {
		select {
		case rsp, ok := <-wch:
			if !ok || rsp.Canceled {
				break
			}

			for _, ev := range rsp.Events {
				kv := ev.Kv
				if kv == nil {
					kv = ev.PrevKv
				}
				if kv == nil {
					continue
				}

				srv, err := registry.Unmarshal(string(kv.Value))
				if err != nil {
					continue
				}

				switch ev.Type {
				case clientv3.EventTypePut:
					cb(&registry.Event{
						Type:    registry.EventUpsert,
						Id:      srv.Id,
						Service: srv,
					})
				case clientv3.EventTypeDelete:
					cb(&registry.Event{
						Type:    registry.EventDelete,
						Id:      srv.Id,
						Service: srv,
					})
				}
			}
		}
	}
}

func (r *_Registry) Close() error {
	r.Lock()
	leases := r.leases
	watchers := r.watcher
	r.leases = nil
	r.watcher = nil
	r.Unlock()

	// unregister
	for key := range leases {
		_, _ = r.client.Delete(context.Background(), r.getKey(key))
	}

	// close watcher
	for _, cancel := range watchers {
		cancel()
	}

	return nil
}

func (r *_Registry) getKey(id string) string {
	return "/registry/" + id
}
