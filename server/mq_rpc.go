package server

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/BitTraceProject/BitTrace-Types/pkg/logger"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

// 消息队列 RPC 服务：根据 exporter 标签写入和读取消息

type (
	MqServer struct {
		address string
	}
	// MqServerAPI 目前只提供同步的接口
	MqServerAPI struct {
		queueSet map[string]*Mq
	}
	Mq struct {
		queue []protocol.MqMessage
	}
)

var (
	mqLogger logger.Logger
)

func init() {
	mqLogger = logger.GetLogger("bittrace_mq")
}

func NewMqServer(addr string) *MqServer {
	return &MqServer{address: addr}
}

func NewMqServerAPI() *MqServerAPI {
	return &MqServerAPI{queueSet: map[string]*Mq{}}
}

func (s *MqServer) Address() string {
	return s.address
}

func (s *MqServer) Run() error {
	mqLogger.Info("===MQ Server Running===")
	// 注册 rpc 服务
	api := NewMqServerAPI()
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
		mqLogger.Info("===Accept New Connect===")
		go func(conn net.Conn) { // 并发处理客户端请求
			jsonrpc.ServeConn(conn)
		}(conn)
	}
}

func (api *MqServerAPI) PushMessage(args *protocol.MqPushMessageArgs, reply *protocol.MqPushMessageReply) error {
	if mq, ok := api.queueSet[args.Message.Tag]; !ok {
		api.queueSet[args.Message.Tag] = &Mq{queue: []protocol.MqMessage{args.Message}}
		reply.OK = true
	} else {
		reply.OK = mq.Enqueue(args.Message)
	}
	mqLogger.Info("[PushMessage]push message by:%s, get reply:%+v", args.Message.Tag, reply)
	return nil
}

func (api *MqServerAPI) FilterMessage(args *protocol.MqFilterMessageArgs, reply *protocol.MqFilterMessageReply) error {
	if mq, ok := api.queueSet[args.Tag]; !ok {
		reply.HasNext = false
		reply.OK = false
	} else {
		msg, ok := mq.Dequeue()
		reply.Message = msg
		reply.HasNext = mq.HasNext()
		reply.OK = ok
	}
	if reply.OK {
		mqLogger.Info("[FilterMessage]filter message by:%s, get reply:%v,%v", args.Tag, reply.HasNext, reply.OK)
	}
	return nil
}

func (api *MqServerAPI) ClearMessage(args *protocol.MqClearMessageArgs, reply *protocol.MqClearMessageReply) error {
	mq, ok := api.queueSet[args.Tag]
	delete(api.queueSet, args.Tag)
	reply.Number, reply.OK = mq.Len(), ok
	mqLogger.Info("[ClearMessage]clear message by:%s, get reply:%d,%v", args.Tag, reply.Number, reply.OK)
	return nil
}

func (mq *Mq) Enqueue(message protocol.MqMessage) bool {
	// TODO 加上队列消息数据量限制，防止 OOM
	mq.queue = append(mq.queue, message)
	return true
}

func (mq *Mq) Dequeue() (protocol.MqMessage, bool) {
	if len(mq.queue) > 0 {
		message := mq.queue[0]
		mq.queue = mq.queue[1:]
		return message, true
	}
	return protocol.MqMessage{}, false
}

func (mq *Mq) HasNext() bool {
	return len(mq.queue) > 0
}

func (mq *Mq) Len() int {
	return len(mq.queue)
}
