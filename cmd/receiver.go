package main

import (
	"github.com/BitTraceProject/BitTrace-Receiver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/gin-gonic/gin"
)

func main() {
	conf := config.ReceiverConfig{
		MetaServerAddr:        "127.0.0.1:8081",
		MqServerAddr:          "127.0.0.1:8082",
		ResolverMgrServerAddr: "127.0.0.1:8083",
	}
	s := server.NewReceiverServer(gin.Default(), conf)

	s.Run(":8080")
}
