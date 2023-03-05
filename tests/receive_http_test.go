package tests

import (
	"net/http"
	"testing"

	"github.com/BitTraceProject/BitTrace-Receiver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"

	"github.com/gin-gonic/gin"
)

func TestReceiveServer(t *testing.T) {
	conf := config.ReceiverConfig{
		MetaServerAddr:        "",
		MqServerAddr:          "",
		ResolverMgrServerAddr: "",
		DatabaseConfig: config.DatabaseConfig{
			Address:  "localhost:33061",
			Username: "admin",
			Password: "admin",
		},
	}
	s := server.NewReceiverServer(gin.Default(), conf)

	s.GET("/", func(context *gin.Context) {
		context.String(http.StatusOK, "OK")
	})

	s.Run(":8080")
}
