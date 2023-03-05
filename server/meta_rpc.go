package server

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"

	"github.com/BitTraceProject/BitTrace-Types/pkg/logger"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

// 元信息管理 RPC 服务：注册 exporter，查询 exporter 对应 resolver，查询 resolver-mgr 等

type (
	MetaServer struct {
		address string
	}
	// MetaServerAPI 目前只提供同步的接口，且目前元信息只在内存中维护一份
	MetaServerAPI struct {
		sync.RWMutex
		exporterInfo map[string]*protocol.ExporterInfo // TODO 这里可以替换为 disk kv
	}
)

var (
	metaLogger logger.Logger
)

func init() {
	metaLogger = logger.GetLogger("bittrace_meta")
}

func NewMetaServer(addr string) *MetaServer {
	return &MetaServer{address: addr}
}

func NewMetaServerAPI() *MetaServerAPI {
	return &MetaServerAPI{exporterInfo: map[string]*protocol.ExporterInfo{}}
}

func (s *MetaServer) Address() string {
	return s.address
}

func (s *MetaServer) Run() error {
	metaLogger.Info("===META Server Running===")
	// 注册 rpc 服务
	api := NewMetaServerAPI()
	err := rpc.Register(api)
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
		metaLogger.Info("===Accept New Connect===")
		go func(conn net.Conn) { // 并发处理客户端请求
			jsonrpc.ServeConn(conn)
		}(conn)
	}
}

func (api *MetaServerAPI) GetExporterInfo(args *protocol.MetaGetExporterInfoArgs, reply *protocol.MetaGetExporterInfoReply) error {
	api.RLock()
	defer api.RUnlock()

	defer func() {
		metaLogger.Info("[GetExporterInfo]get exporter info by:%s, get reply:%+v", args.Key, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		reply.Info = *info
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) GetExporterResolver(args *protocol.MetaGetExporterResolverArgs, reply *protocol.MetaGetExporterResolverReply) error {
	api.RLock()
	defer api.RUnlock()

	defer func() {
		metaLogger.Info("[GetExporterResolver]get exporter resolver by:%s, get reply:%+v", args.Key, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		reply.ResolverTag = info.ResolverTag
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) ExporterHasJoin(args *protocol.MetaExporterHasJoinArgs, reply *protocol.MetaExporterHasJoinReply) error {
	api.RLock()
	defer api.RUnlock()

	defer func() {
		metaLogger.Info("[ExporterHasJoin]get exporter has join by:%s, get reply:%+v", args.Key, reply)
	}()

	if _, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) GetExporterTable(args *protocol.MetaGetExporterTableArgs, reply *protocol.MetaGetExporterTableReply) error {
	api.RLock()
	defer api.RUnlock()

	defer func() {
		metaLogger.Info("[GetExporterTable]get exporter table by:%s, get reply:%+v", args.Key, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		reply.Table = info.Table
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) GetExporterStatus(args *protocol.MetaGetExporterStatusArgs, reply *protocol.MetaGetExporterStatusReply) error {
	api.RLock()
	defer api.RUnlock()

	defer func() {
		metaLogger.Info("[GetExporterStatus]get exporter status by:%s, get reply:%+v", args.Key, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		reply.StatusCode = info.StatusCode
		reply.JoinTimestamp = info.JoinTimestamp
		reply.QuitTimestamp = info.QuitTimestamp
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) GetExporterProgress(args *protocol.MetaGetExporterProgressArgs, reply *protocol.MetaGetExporterProgressReply) error {
	api.RLock()
	defer api.RUnlock()

	defer func() {
		metaLogger.Info("[GetExporterProgress]get exporter progress by:%s, get reply:%+v", args.Key, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		reply.CurrentProgress = info.CurrentProgress
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) GetAllExporterInfo(args *protocol.MetaGetAllExporterInfoArgs, reply *protocol.MetaGetAllExporterInfoReply) error {
	api.RLock()
	defer api.RUnlock()

	defer func() {
		metaLogger.Info("[GetAllExporterInfo]get exporter info:%d", len(api.exporterInfo))
	}()

	exporterInfo := make(map[string]protocol.ExporterInfo)
	for key, info := range api.exporterInfo {
		exporterInfo[key] = *info
	}
	reply.ExporterInfo = exporterInfo
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) NewExporterInfo(args *protocol.MetaNewExporterInfoArgs, reply *protocol.MetaNewExporterInfoReply) error {
	api.Lock()
	defer api.Unlock()

	defer func() {
		metaLogger.Info("[NewExporterInfo]new exporter info by:%+v, get reply:%+v", args, reply)
	}()

	// 已存在则不会新建，直接跳过
	if _, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		return nil
	}
	api.exporterInfo[args.Key] = &args.Info
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) UpdateExporterStatus(args *protocol.MetaUpdateExporterStatusArgs, reply *protocol.MetaUpdateExporterStatusReply) error {
	api.Lock()
	defer api.Unlock()

	defer func() {
		metaLogger.Info("[UpdateExporterStatus]update exporter status by:%+v, get reply:%+v", args, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		info.StatusCode = args.StatusCode
		info.QuitTimestamp = args.QuitTimestamp
		api.exporterInfo[args.Key] = info
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) UpdateExporterProgress(args *protocol.MetaUpdateExporterProgressArgs, reply *protocol.MetaUpdateExporterProgressReply) error {
	api.Lock()
	defer api.Unlock()

	defer func() {
		metaLogger.Info("[UpdateExporterProgress]update exporter progress by:%+v, get reply:%+v", args, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		info.CurrentProgress = args.CurrentProgress
		api.exporterInfo[args.Key] = info
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) DeleteExporterInfo(args *protocol.MetaDeleteExporterInfoArgs, reply *protocol.MetaDeleteExporterInfoReply) error {
	api.Lock()
	defer api.Unlock()

	defer func() {
		metaLogger.Info("[DeleteExporterInfo]delete exporter info by:%s, get reply:%+v", args.Key, reply)
	}()

	if info, ok := api.exporterInfo[args.Key]; ok {
		reply.HasJoin = true
		reply.OK = true
		reply.Info = *info
		delete(api.exporterInfo, args.Key)
		return nil
	}
	reply.HasJoin = false
	reply.OK = true
	return nil
}

func (api *MetaServerAPI) ClearAllExporterInfo(args *protocol.MetaClearAllExporterInfoArgs, reply *protocol.MetaClearAllExporterInfoReply) error {
	api.Lock()
	defer api.Unlock()

	defer func() {
		metaLogger.Info("[ClearAllExporterInfo]clear all exporter info, get reply:%+v", reply)
	}()

	reply.OK = true
	reply.Number = len(api.exporterInfo)
	api.exporterInfo = map[string]*protocol.ExporterInfo{}
	return nil
}
