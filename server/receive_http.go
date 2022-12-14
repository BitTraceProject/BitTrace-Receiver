package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"

	"github.com/gin-gonic/gin"
)

// 快照接收 HTTP 服务：接收来自 exporter 的原始数据

type (
	ReceiverServer struct {
		*gin.Engine

		conf config.ReceiverConfig

		metaClient        *rpc.Client
		mqClient          *rpc.Client
		resolverMgrClient *rpc.Client
	}
)

const (
	// Exporter Server
	exporterPath = "/exporter"
	joinPath     = "/join"
	dataPath     = "/data"
	quitPath     = "/quit"

	exporterTagKey = "exporter_tag"
	lazyQuitKey    = "lazy_quit"
)

var (
	helpTextIndex    = fmt.Sprintf("Exporter Server Path: %s", exporterPath)
	helpTextExporter = fmt.Sprintf(`RelativePaths of Exporter Server include:
	- %s, join a exporter.
	- %s, receive data package from a identity exporter.
	- %s, quit a exporter."`,
		joinPath,
		dataPath,
		quitPath)
)

func NewReceiverServer(r *gin.Engine, conf config.ReceiverConfig) *ReceiverServer {
	s := &ReceiverServer{
		Engine: r,
		conf:   conf,
	}
	s.register()
	return s
}

func (s *ReceiverServer) register() {
	s.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, helpTextIndex)
	})
	r := s.Group(exporterPath)
	r.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, helpTextExporter)
	})
	r.GET(joinPath, s.joinHandleFunc)
	r.POST(dataPath, s.dataHandleFunc)
	r.GET(quitPath, s.quitHandleFunc)
}

func (s *ReceiverServer) joinHandleFunc(c *gin.Context) {
	var resp = new(protocol.ReceiverJoinResponse)
	defer func(resp *protocol.ReceiverJoinResponse) {
		// 最后响应
		c.JSON(http.StatusOK, *resp)
	}(resp)
	// 读取请求 exporter tag
	exporterTag := c.Query(exporterTagKey)
	// 根据 tag 查询 exporter 是否已注册，调用 meta
	getValueArgs := &protocol.MetaGetValueArgs{Key: exporterTag}
	var getValueReply protocol.MetaGetValueReply
	err := s.CallMetaServer("MetaServerAPI.GetValue", getValueArgs, &getValueReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call meta error:%v", err)
		return
	}
	if getValueReply.OK {
		resp.OK = true
		resp.Msg = getValueReply.Value
		return
	}
	// 如果未注册，则先为其分配 resolver，调用 resolver-mgr
	startArgs := &protocol.ResolverStartArgs{ExporterTag: exporterTag}
	var startReply protocol.ResolverStartReply
	err = s.CallResolverMgrServer("ResolverMgrServerAPI.Start", startArgs, &startReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call resolver mgr error:%v", err)
		return
	}
	// 更新元信息，调用 meta
	setValueArgs := &protocol.MetaSetValueArgs{Key: exporterTag, Value: startReply.ResolverTag}
	var setValueReply protocol.MetaSetValueReply
	err = s.CallMetaServer("MetaServerAPI.SetValue", setValueArgs, &setValueReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call meta error:%v", err)
		return
	}
	// 返回 OK
	resp.OK = true
	resp.Msg = startReply.ResolverTag
	return
}

func (s *ReceiverServer) dataHandleFunc(c *gin.Context) {
	var resp = new(protocol.ReceiverDataResponse)
	// 读取请求详情
	body := c.Request.Body
	dataAsBytes, err := io.ReadAll(body)
	if err != nil {
		log.Printf("[dataHandleFunc]read body err:%v", err)
		resp.OK = false
		c.JSON(http.StatusOK, *resp)
		return
	}
	var req protocol.ReceiverDataRequest
	err = json.Unmarshal(dataAsBytes, &req)
	if err != nil {
		log.Printf("[dataHandleFunc]json err:%v", err)
		resp.OK = false
		c.JSON(http.StatusOK, *resp)
		return
	}
	resp.OK = true
	c.JSON(http.StatusOK, *resp)
	// 异步完成后续处理
	go func(exporterTag string, dataPackage protocol.ReceiverDataPackage) {
		data, err := json.Marshal(dataPackage)
		if err != nil {
			log.Printf("[dataHandleFunc]json error:%v", err)
			return
		}
		// 根据 tag 验证 exporter 是否已注册，异步调用 meta
		getValueArgs := &protocol.MetaGetValueArgs{Key: exporterTag}
		var getValueReply protocol.MetaGetValueReply
		err = s.CallMetaServer("MetaServerAPI.GetValue", getValueArgs, &getValueReply)
		if err != nil {
			// TODO 这里加上重试机制，尽量保证不会出错
			log.Printf("[dataHandleFunc]call meta error:%v", err)
			return
		}
		// 如果 exporter 未注册直接返回不处理
		// TODO 由于是内部服务，所以这里不必考虑 DoS 攻击
		if !getValueReply.OK {
			log.Printf("[dataHandleFunc]get value not ok:%v", getValueReply)
			return
		}
		// 根据对应 resolver，将数据放入 mq，异步调用 mq
		resolverTag := getValueReply.Value
		message := protocol.MqMessage{
			Tag: resolverTag,
			Msg: data,
		}
		pushMessageArgs := &protocol.MqPushMessageArgs{Message: message}
		var pushMessageReply protocol.MqPushMessageReply
		err = s.CallMqServer("MqServerAPI.PushMessage", pushMessageArgs, &pushMessageReply)
		if err != nil {
			// TODO 这里加上重试机制，尽量保证不会出错
			log.Printf("[dataHandleFunc]call mq error:%v", err)
		}
	}(req.ExporterTag, req.DataPackage)
}

func (s *ReceiverServer) quitHandleFunc(c *gin.Context) {
	var resp = new(protocol.ReceiverQuitResponse)
	// 读取请求 exporter tag
	exporterTag := c.Query(exporterTagKey)
	lazyQuit := false
	lazyQuitValue := c.Query(lazyQuitKey)
	if lazyQuitValue == "true" {
		lazyQuit = true
	}
	// 根据 tag 查询 exporter 是否已注册，调用 meta
	getValueArgs := &protocol.MetaGetValueArgs{Key: exporterTag}
	var getValueReply protocol.MetaGetValueReply
	err := s.CallMetaServer("MetaServerAPI.GetValue", getValueArgs, &getValueReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[quitHandleFunc]call meta error:%v", err)
		c.JSON(http.StatusOK, *resp)
		return
	}
	// 不管是否已注册，都直接返回
	if getValueReply.OK {
		resp.OK = true
		resp.Msg = getValueReply.Value
	} else {
		resp.OK = false
		resp.Msg = "exporter not existed"
	}
	c.JSON(http.StatusOK, *resp)
	go func(resolverTag string, lazyShutdown bool) {
		// 关闭对应 resolver，调用 resolver-mgr
		shutdownArgs := &protocol.ResolverShutdownArgs{ExporterTag: exporterTag, LazyShutdown: lazyShutdown}
		var shutdownReply protocol.ResolverShutdownReply
		err := s.CallResolverMgrServer("ResolverMgrServerAPI.Shutdown", shutdownArgs, &shutdownReply)
		if err != nil {
			// TODO 这里加上重试机制，尽量保证不会出错
			log.Printf("[quitHandleFunc]call resolver mgr error:%v", err)
			// 这里出错不影响后面，继续
		}
		// 更新元信息，调用 meta
		delKeyArgs := &protocol.MetaDelKeyArgs{Key: exporterTag}
		var delKeyReply protocol.MetaDelKeyReply
		err = s.CallMetaServer("MetaServerAPI.DelKey", delKeyArgs, &delKeyReply)
		if err != nil {
			// TODO 这里加上重试机制，尽量保证不会出错
			log.Printf("[quitHandleFunc]call meta error:%v", err)
		}
	}(getValueReply.Value, lazyQuit)
}

func (s *ReceiverServer) CallMetaServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if s.metaClient == nil {
			s.metaClient, err = jsonrpc.Dial("tcp", s.conf.MetaServerAddr)
			if err != nil {
				return err
			}
		}
		err = s.metaClient.Call(serviceMethod, args, reply)
		if err != nil {
			s.metaClient = nil
		}
		return err
	})
}

func (s *ReceiverServer) CallMqServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if s.mqClient == nil {
			s.mqClient, err = jsonrpc.Dial("tcp", s.conf.MqServerAddr)
			if err != nil {
				return err
			}
		}
		err = s.mqClient.Call(serviceMethod, args, reply)
		if err != nil {
			s.mqClient = nil
		}
		return err
	})
}

func (s *ReceiverServer) CallResolverMgrServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if s.resolverMgrClient == nil {
			s.resolverMgrClient, err = jsonrpc.Dial("tcp", s.conf.ResolverMgrServerAddr)
			if err != nil {
				return err
			}
		}
		err = s.resolverMgrClient.Call(serviceMethod, args, reply)
		if err != nil {
			s.resolverMgrClient = nil
		}
		return err
	})
}
