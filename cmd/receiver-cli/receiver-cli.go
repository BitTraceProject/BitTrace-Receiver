package main

import (
	"fmt"
	"log"

	"github.com/BitTraceProject/BitTrace-Receiver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"

	"github.com/gin-gonic/gin"
)

func main() {
	envModule, err := common.LookupEnv("MODULE")
	if err != nil {
		panic(err)
	}
	selectModule(envModule)
}

func selectModule(module string) {
	switch module {
	case "receiver":
		runReceiver()
	case "mq":
		runMq()
	case "meta":
		runMeta()
	default:
		panic(fmt.Sprintf("error: unknown module:%s", module))
	}
}

func runReceiver() {
	conf := config.ReceiverConfig{
		MqServerAddr:          "mq.receiver.bittrace.proj:8081",
		MetaServerAddr:        "meta.receiver.bittrace.proj:8082",
		ResolverMgrServerAddr: "mgr.resolver.bittrace.proj:8083",
		DatabaseConfig: config.DatabaseConfig{
			Address:  "master.collector.bittrace.proj:33061",
			Username: "admin",
			Password: "admin",
		},
	}
	s := server.NewReceiverServer(gin.Default(), conf)

	log.Println("running receiver")
	s.Run(":8080")
}

func runMq() {
	s := server.NewMqServer(":8081")

	log.Println("running mq")
	s.Run()
}

func runMeta() {
	s := server.NewMetaServer(":8082")

	log.Println("running meta")
	s.Run()
}
