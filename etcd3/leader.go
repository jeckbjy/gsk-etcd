package etcd3

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/jeckbjy/gsk/sync/leader"
)

func NewLeader() leader.Leader {
	l := &_Leader{}
	return l
}

type _Leader struct {
	client *clientv3.Client
}

func (l *_Leader) Elect(id string, opts ...leader.ElectOption) (leader.Elected, error) {
	return nil, nil
}

func (l *_Leader) Follow() chan string {
	return nil
}

type _Elected struct {
}

func (e *_Elected) Id() string {
	return ""
}

func (e *_Elected) Reelect() error {
	return nil
}

func (e *_Elected) Resign() error {
	return nil
}

func (e *_Elected) Revoked() chan bool {
	return nil
}
