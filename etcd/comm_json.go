package etcd

type storeInfo struct {
	Tags    map[string]string
	Network string
	Addr    string
	Weight  int
}
