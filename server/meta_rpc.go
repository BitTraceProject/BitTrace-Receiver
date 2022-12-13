package server

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

// 元信息管理 RPC 服务：注册 exporter，查询 exporter 对应 resolver，查询 resolver-mgr 等

type (
	MetaServer struct {
		address string
	}
	// MetaServerAPI 目前只提供同步的接口，且目前元信息只在内存中维护一份
	MetaServerAPI struct {
		kv map[string]string // TODO 这里可以替换为 disk kv
	}
)

func NewMetaServer(addr string) *MetaServer {
	return &MetaServer{address: addr}
}

func NewMetaServerAPI() *MetaServerAPI {
	return &MetaServerAPI{kv: map[string]string{}}
}

func (s *MetaServer) Address() string {
	return s.address
}

func (s *MetaServer) Run() error {
	// 注册 rpc 服务
	api := NewMetaServerAPI()
	// 将 meta server 地址写入
	err := api.SetValue(&protocol.MetaSetValueArgs{
		Key:   "META_SERVER",
		Value: s.address,
	}, &protocol.MetaSetValueReply{})
	if err != nil {
		return fmt.Errorf("[Run]fatal error:%v", err)
	}
	err = rpc.Register(api)
	if err != nil {
		return fmt.Errorf("[Run]fatal error:%v", err)
	}

	l, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("[Run]fatal error:%v", err)
	}

	for {
		conn, err := l.Accept() // 接收客户端连接请求
		if err != nil {
			continue
		}

		go func(conn net.Conn) { // 并发处理客户端请求
			jsonrpc.ServeConn(conn)
		}(conn)
	}
}

func (api *MetaServerAPI) GetKey(args *protocol.MetaGetKeyArgs, reply *protocol.MetaGetKeyReply) error {
	_, ok := api.kv[args.Key]
	reply.OK = ok
	return nil
}

func (api *MetaServerAPI) DelKey(args *protocol.MetaDelKeyArgs, reply *protocol.MetaDelKeyReply) error {
	_, ok := api.kv[args.Key]
	reply.OK = ok
	delete(api.kv, args.Key)
	return nil
}

func (api *MetaServerAPI) GetValue(args *protocol.MetaGetValueArgs, reply *protocol.MetaGetValueReply) error {
	value, ok := api.kv[args.Key]
	reply.Value, reply.OK = value, ok
	return nil
}

func (api *MetaServerAPI) SetValue(args *protocol.MetaSetValueArgs, reply *protocol.MetaSetValueReply) error {
	_, ok := api.kv[args.Key]
	reply.OK = ok
	api.kv[args.Key] = args.Value
	return nil
}

func (api *MetaServerAPI) Clear(args *protocol.MetaClearArgs, reply *protocol.MetaClearReply) error {
	api.kv = make(map[string]string)
	return nil
}
