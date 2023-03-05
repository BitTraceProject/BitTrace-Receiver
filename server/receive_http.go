package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/database"
	"github.com/BitTraceProject/BitTrace-Types/pkg/logger"
	"github.com/BitTraceProject/BitTrace-Types/pkg/metric"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// 快照接收 HTTP 服务：接收来自 exporter 的原始数据

type (
	ReceiverServer struct {
		*gin.Engine

		conf config.ReceiverConfig

		metaClient        *rpc.Client
		mqClient          *rpc.Client
		resolverMgrClient *rpc.Client

		masterDBInst *gorm.DB
	}
)

const (
	// Exporter Server
	exporterPath = "/exporter"
	joinPath     = "/join"
	queryPath    = "/query"
	dataPath     = "/data"
	quitPath     = "/quit"

	exporterTagKey = "exporter_tag"
	lazyQuitKey    = "lazy_quit"
)

var (
	helpTextIndex    = fmt.Sprintf("Exporter Server Path: %s", exporterPath)
	helpTextExporter = fmt.Sprintf(`RelativePaths of Exporter Server include:
	- %s, join a exporter.
	- %s, query a exporter.
	- %s, receive data package from a identity exporter.
	- %s, quit a exporter."`,
		joinPath,
		queryPath,
		dataPath,
		quitPath)

	receiverLogger logger.Logger
)

func init() {
	receiverLogger = logger.GetLogger("bittrace_receiver")
}

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
	r.GET(queryPath, s.queryHandleFunc)
	r.POST(dataPath, s.dataHandleFunc)
	r.GET(quitPath, s.quitHandleFunc)
}

func (s *ReceiverServer) joinHandleFunc(c *gin.Context) {
	// 1 获取 tag，然后到 meta 查询是否存在和状态，如果存在：状态为 active 直接返回，状态为其他，返回错误；不存在，则继续

	// 2 初始化一个空 info，开始依次填充字段值: resolver mgr start, collector create table, join timestamp, status code.

	// 3 更新元信息

	var resp = new(protocol.ReceiverJoinResponse)
	defer func(resp *protocol.ReceiverJoinResponse) {
		// 最后响应
		receiverLogger.Info("[joinHandleFunc]get response:%+v", *resp)
		c.JSON(http.StatusOK, *resp)
	}(resp)
	exporterTag := c.Query(exporterTagKey)
	receiverLogger.Info("[joinHandleFunc]join new exporter:%s", exporterTag)

	exporterInfoKey := common.GenExporterInfoKey(exporterTag)
	getInfoArgs := &protocol.MetaGetExporterInfoArgs{Key: exporterInfoKey}
	var getInfoReply protocol.MetaGetExporterInfoReply
	err := s.CallMetaServer("MetaServerAPI.GetExporterInfo", getInfoArgs, &getInfoReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call meta get exporter info error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	if !getInfoReply.OK {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call meta get exporter info not ok")
		return
	}
	if getInfoReply.HasJoin {
		// tag 已注册
		if getInfoReply.Info.StatusCode == protocol.StatusActive {
			resp.OK = true
			resp.Msg = getInfoReply.Info.ResolverTag
			resp.Info = getInfoReply.Info
		} else {
			// 返回错误：exporter 未加入但是未正常退出，目前对于这种异常状态 receiver 不做处理
			// TODO 对于某些异常状态，receiver 可以做针对处理，如 dead
			resp.OK = false
			resp.Msg = fmt.Sprintf("[joinHandleFunc]exporter has join, but status[%v] not active", getInfoReply.Info.StatusCode)
			resp.Info = getInfoReply.Info
		}
		return
	}

	var (
		info = protocol.ExporterInfo{}
	)
	info.JoinTimestamp = common.FromNow()
	table := protocol.ExporterTable{
		TableNameSnapshotData: common.GenSnapshotDataTableName(exporterTag, info.JoinTimestamp),
		TableNameSnapshotSync: common.GenSnapshotSyncTableName(exporterTag, info.JoinTimestamp),
		TableNameState:        common.GenStateTableName(exporterTag, info.JoinTimestamp),
		TableNameRevision:     common.GenRevisionTableName(exporterTag, info.JoinTimestamp),
		TableNameEventOrphan:  common.GenEventOrphanTableName(exporterTag, info.JoinTimestamp),
	}
	info.Table = table

	sqlList := []string{
		database.SqlCreateTableSnapshotData(table.TableNameSnapshotData),
		database.SqlCreateTableSnapshotSync(table.TableNameSnapshotSync),
		database.SqlCreateTableState(table.TableNameState),
		database.SqlCreateTableRevision(table.TableNameRevision),
		database.SqlCreateTableEventOrphan(table.TableNameEventOrphan),
	}
	s.masterDBInst, err = database.TryExecPipelineSql(s.masterDBInst, sqlList, s.conf.DatabaseConfig)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call database exec pipeline error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}

	// TODO 已注册，这里也应该启动 resolver，看看应该怎么解决
	// 因为 receiver 处查询 exporter 存在不会调用 start 接口，
	// 那么 resolver-mgr 如果重启的话，丢失的 resolver 需要一种方式来恢复
	startArgs := &protocol.ResolverStartArgs{ExporterTag: exporterTag}
	var startReply protocol.ResolverStartReply
	err = s.CallResolverMgrServer("ResolverMgrServerAPI.Start", startArgs, &startReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call resolver mgr start error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	if !startReply.OK {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call resolver mgr start not ok")
		return
	}
	info.ResolverTag = startReply.ResolverTag
	info.StatusCode = protocol.StatusActive

	// 更新元信息，调用 meta
	newInfoArgs := &protocol.MetaNewExporterInfoArgs{Key: exporterInfoKey, Info: info}
	var newInfoReply protocol.MetaNewExporterInfoReply
	err = s.CallMetaServer("MetaServerAPI.NewExporterInfo", newInfoArgs, &newInfoReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call meta new exporter info[%+v] error:%v", info, err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	if !newInfoReply.OK {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[joinHandleFunc]call meta new exporter info[%+v] not ok", info)
		return
	}
	// 返回 OK
	resp.OK = true
	resp.Msg = startReply.ResolverTag
	resp.Info = info
	return
}

func (s *ReceiverServer) queryHandleFunc(c *gin.Context) {
	var resp = new(protocol.ReceiverQueryResponse)
	defer func(resp *protocol.ReceiverQueryResponse) {
		// 最后响应
		receiverLogger.Info("[queryHandleFunc]get response:%+v", *resp)
		c.JSON(http.StatusOK, *resp)
	}(resp)
	exporterTag := c.Query(exporterTagKey)
	receiverLogger.Info("[queryHandleFunc]query exporter:%s", exporterTag)

	exporterInfoKey := common.GenExporterInfoKey(exporterTag)
	getInfoArgs := &protocol.MetaGetExporterInfoArgs{Key: exporterInfoKey}
	var getInfoReply protocol.MetaGetExporterInfoReply
	err := s.CallMetaServer("MetaServerAPI.GetExporterInfo", getInfoArgs, &getInfoReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[queryHandleFunc]call meta get exporter info error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	if !getInfoReply.OK {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[queryHandleFunc]call meta get exporter info not ok")
		return
	}
	if !getInfoReply.HasJoin {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[queryHandleFunc]exporter has not join")
		return
	}
	// 返回 OK
	resp.OK = true
	resp.Msg = getInfoReply.Info.ResolverTag
	resp.Info = getInfoReply.Info
	return
}

func (s *ReceiverServer) dataHandleFunc(c *gin.Context) {
	var resp = new(protocol.ReceiverDataResponse)
	defer func(resp *protocol.ReceiverDataResponse) {
		// 最后响应
		receiverLogger.Info("[dataHandleFunc]get response:%+v", *resp)
		c.JSON(http.StatusOK, *resp)
	}(resp)
	// 读取请求后，数据异步写入 mq，并更新进度，注意：进度信息已 exporter 上传来的为准，meta 存的只用于 join 时的检查恢复

	// 读取请求详情
	body := c.Request.Body
	dataAsBytes, err := io.ReadAll(body)
	if err != nil {
		receiverLogger.Error("[dataHandleFunc]read body err:%v", err)
		resp.OK = false

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       "[dataHandleFunc]read body err",
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	var req protocol.ReceiverDataRequest
	err = json.Unmarshal(dataAsBytes, &req)
	if err != nil {
		receiverLogger.Error("[dataHandleFunc]json err:%v", err)
		resp.OK = false
		return
	}
	receiverLogger.Info("[dataHandleFunc]post new data from:%s,progress:%+v", req.ExporterTag, req.CurrentProgress)

	data, err := json.Marshal(req.DataPackage)
	if err != nil {
		receiverLogger.Error("[dataHandleFunc]json marshal error:%v", err)
		resp.OK = false

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       "[dataHandleFunc]json marshal error",
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	// 根据 tag 验证 exporter 是否已注册，调用 meta
	exporterInfoKey := common.GenExporterInfoKey(req.ExporterTag)
	getResolverArgs := &protocol.MetaGetExporterResolverArgs{Key: exporterInfoKey}
	var getResolverReply protocol.MetaGetExporterResolverReply
	err = s.CallMetaServer("MetaServerAPI.GetExporterResolver", getResolverArgs, &getResolverReply)
	if err != nil {
		receiverLogger.Error("[dataHandleFunc]call meta get exporter resolver error:%v", err)
		resp.OK = false

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       req.ExporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	if !getResolverReply.OK {
		receiverLogger.Error("[dataHandleFunc]call meta get exporter resolver not ok")
		resp.OK = false
		return
	}
	// 如果 exporter 未注册直接返回不处理
	if !getResolverReply.HasJoin {
		receiverLogger.Error("[dataHandleFunc]exporter has not join")
		resp.OK = false
		return
	}

	// 根据对应 resolver，将数据放入 mq，异步调用 mq
	message := protocol.MqMessage{
		Tag: getResolverReply.ResolverTag,
		Msg: data,
	}
	pushMessageArgs := &protocol.MqPushMessageArgs{Message: message}
	var pushMessageReply protocol.MqPushMessageReply
	err = s.CallMqServer("MqServerAPI.PushMessage", pushMessageArgs, &pushMessageReply)
	if err != nil {
		receiverLogger.Error("[dataHandleFunc]call mq push message error:%v", err)
		resp.OK = false

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       req.ExporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}

	updateProgressArgs := &protocol.MetaUpdateExporterProgressArgs{Key: exporterInfoKey, CurrentProgress: req.CurrentProgress}
	var updateProgressReply protocol.MetaUpdateExporterProgressReply
	err = s.CallMetaServer("MetaServerAPI.UpdateExporterProgress", updateProgressArgs, &updateProgressReply)
	if err != nil {
		receiverLogger.Error("[dataHandleFunc]call meta update progress[%v] error:%v", req.CurrentProgress, err)
		// 继续进行，直接返回 true
	}
	if !updateProgressReply.OK {
		receiverLogger.Error("[dataHandleFunc]call meta update progress[%v] not ok", req.CurrentProgress)
		// 继续进行，直接返回 true
	}
	resp.OK = true
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
	receiverLogger.Info("[quitHandleFunc]quit exporter:%s,lazy:%v", exporterTag, lazyQuit)

	// 根据 tag 查询 exporter 是否已注册，调用 meta
	exporterInfoKey := common.GenExporterInfoKey(exporterTag)
	getResolverArgs := &protocol.MetaGetExporterResolverArgs{Key: exporterInfoKey}
	var getResolverReply protocol.MetaGetExporterResolverReply
	err := s.CallMetaServer("MetaServerAPI.GetExporterResolver", getResolverArgs, &getResolverReply)
	if err != nil {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[quitHandleFunc]call meta get exporter resolver error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeReceiver,
		})
		return
	}
	if !getResolverReply.OK {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[quitHandleFunc]call meta get exporter resolver not ok")
		return
	}
	if !getResolverReply.HasJoin {
		resp.OK = false
		resp.Msg = fmt.Sprintf("[quitHandleFunc]exporter has not join")
		return
	}

	resp.OK = true
	c.JSON(http.StatusOK, *resp)
	receiverLogger.Info("[quitHandleFunc]get response:%+v", *resp)

	go func(exporterTag, resolverTag string, lazyShutdown bool) {
		// 关闭对应 resolver，调用 resolver-mgr
		shutdownArgs := &protocol.ResolverShutdownArgs{ExporterTag: exporterTag, LazyShutdown: lazyShutdown}
		var shutdownReply protocol.ResolverShutdownReply
		err := s.CallResolverMgrServer("ResolverMgrServerAPI.Shutdown", shutdownArgs, &shutdownReply)
		if err != nil {
			receiverLogger.Error("[quitHandleFunc]call resolver mgr error:%v", err)
			// 这里出错不影响后面，继续

			// metric
			metric.MetricLogModuleError(metric.MetricModuleError{
				Tag:       exporterTag,
				Timestamp: common.FromNow().String(),
				Module:    metric.ModuleTypeReceiver,
			})
		}
		// 如果不是 lazy shutdown，直接删除，否则在 resolver 处会处理
		if !lazyShutdown {
			// 更新元信息，调用 meta
			exporterInfoKey := common.GenExporterInfoKey(exporterTag)
			delInfoArgs := &protocol.MetaDeleteExporterInfoArgs{Key: exporterInfoKey}
			var delInfoReply protocol.MetaDeleteExporterInfoReply
			err = s.CallMetaServer("MetaServerAPI.DeleteExporterInfo", delInfoArgs, &delInfoReply)
			if err != nil {
				receiverLogger.Error("[quitHandleFunc]call meta del exporter info error:%v", err)

				// metric
				metric.MetricLogModuleError(metric.MetricModuleError{
					Tag:       exporterTag,
					Timestamp: common.FromNow().String(),
					Module:    metric.ModuleTypeReceiver,
				})
			}
		}
	}(exporterTag, getResolverReply.ResolverTag, lazyQuit)
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
