**v1.8.0** (2022/04/26) 

- 支持动态公共属性 
- 支持首次事件 
- 新增user_uniq_append事件

**v1.7.0** (2021/12/29)

- 增加支持复杂结构类型

**v1.6.2** (2021/08/05)

- 修复flush方法没有刷新文件日期的问题

**v1.6.1** (2021/06/04)

- 修改LoggingConsumer默认写文件的条件为5条数据写入一次
- 修改BatchConsumer默认最大缓存区上限为50批次

**v1.6.0** (2021/03/22)

- 新增网络失败重试机制
- 增加缓存区，重试3次发送失败的数据将存入缓存区中，缓存数据超过上限将会丢弃早期数据
- 增加#app_id属性

**v1.5.0** (2020/11/21)

- LoggingConsumer 支持自动创建目录

**v1.4.0** (2020/08/27)

- 新增track_update接口，支持可更新事件
- 新增track_overwrite接口，支持可重写事件

**v1.3.3** (2020/08/12)

- 修复AsyncBatchConsumer在特殊情况下线程不释放的问题

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
