package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdResolver struct {
	etcdClient *clientv3.Client
}

func NewEtcdResolver(endpoints []string) (discovery.Resolver, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		return nil, err
	}

	return &etcdResolver{
		etcdClient: etcdClient,
	}, nil
}

func NewEtcdResolverWithAuth(endpoints []string, username string, password string) (discovery.Resolver, error) {
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
		Username:  username,
		Password:  password,
	})
	if err != nil {
		return nil, err
	}

	return &etcdResolver{
		etcdClient: etcdClient,
	}, nil
}

func (r *etcdResolver) Target(ctx context.Context, target rpcinfo.EndpointInfo) (description string) {
	return target.ServiceName()
}

func (r *etcdResolver) Resolve(ctx context.Context, desc string) (discovery.Result, error) {
	result := discovery.Result{}

	resp, err := r.etcdClient.Get(ctx, fmt.Sprintf("kitex-registry/%s", desc), clientv3.WithPrefix())
	if err != nil {
		return result, err
	}

	for _, v := range resp.Kvs {
		info := storeInfo{}
		err := json.Unmarshal(v.Value, &info)
		if err != nil {
			log.Printf("fail to unmarshal with err: %v, ignore key: %v\n", err, v.Key)
			continue
		}

		w := info.Weight
		if w <= 0 {
			w = discovery.DefaultWeight
		}

		ins := discovery.NewInstance(info.Network, info.Addr, info.Weight, info.Tags)
		result.Instances = append(result.Instances, ins)
	}

	if len(result.Instances) == 0 {
		return result, errors.New("no instance remains for: " + desc)
	}

	result.CacheKey = desc
	result.Cacheable = true
	return result, nil
}

func (r *etcdResolver) Diff(cacheKey string, prev, next discovery.Result) (discovery.Change, bool) {
	return discovery.DefaultDiff(cacheKey, prev, next)
}

// Name implements the Resolver interface.
func (r *etcdResolver) Name() string {
	return "etcd"
}
