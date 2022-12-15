package server

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"

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
	return nil
}

func (api *MqServerAPI) FilterMessage(args *protocol.MqFilterMessageArgs, reply *protocol.MqFilterMessageReply) error {
	if mq, ok := api.queueSet[args.Tag]; !ok {
		reply.HasNext = false
		reply.OK = false
	} else {
		reply.Message = mq.Dequeue()
		reply.HasNext = mq.HasNext()
		reply.OK = true
	}
	return nil
}

func (api *MqServerAPI) ClearMessage(args *protocol.MqClearMessageArgs, reply *protocol.MqClearMessageReply) error {
	mq, ok := api.queueSet[args.Tag]
	delete(api.queueSet, args.Tag)
	reply.Number, reply.OK = mq.Len(), ok
	return nil
}

func (mq *Mq) Enqueue(message protocol.MqMessage) bool {
	// TODO 加上队列消息数据量限制，防止 OOM
	mq.queue = append(mq.queue, message)
	return true
}

func (mq *Mq) Dequeue() protocol.MqMessage {
	message := mq.queue[0]
	mq.queue = mq.queue[1:]
	return message
}

func (mq *Mq) HasNext() bool {
	return len(mq.queue) > 0
}

func (mq *Mq) Len() int {
	return len(mq.queue)
}
