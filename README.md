# BitTrace-Receiver

Receiver 的组成和功能包括：
- Receive HTTP 服务：接收来自 Exporter 的原始数据，为其分配 Resolver，并转发给 Resolver
- Meta RPC 服务：Receiver 根据 Exporter 传来的数据会维护一个 Exporter 与 Resolver 的对应拓扑关系，供 Resolver 查询，用于从 MQ 中读取对应 Exporter 的消息，Meta 还会维护其他的必要的元信息
- MQ RPC 服务：多个 Exporter 发来的消息会放入同一个 MQ 中，供不同的 Resolver 消费

