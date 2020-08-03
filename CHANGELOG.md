**v1.3.2** (2020/08/03)
- 支持 LoggingConsumer 配置文件前缀名
- 新增UUID配置开关

**v1.3.1** (2020/02/26)
- 兼容python2的DebugConsumer错误日志打印

**v1.3.0** (2020/02/11)
- 数据类型支持list类型
- 新增 user_append 接口，支持用户的数组类型的属性追加
- 新增 user_unset 接口，支持删除用户属性
- BatchConsumer 性能优化：支持选择是否压缩；移除 Base64 编码
- DebugConsumer 优化: 在服务端对数据进行更完备准确地校验

**v1.2.0** (2019/09/25)
- 支持 DebugConsumer
- 支持日志文件按小时切分
- BatchConsumer 支持多线程
- 修复AsyncBatchConsumer 批量发送不生效的问题
- 优化数据上报返回码处理逻辑
- LoggingConsumer: 默认不限制单个日志文件大小
