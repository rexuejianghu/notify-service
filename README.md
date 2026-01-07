# notify-service
# 异步通知服务 - 使用 Kafka

基于 Spring Boot 的异步通知服务，使用 Kafka 作为消息队列，实现任务的顺序判断、重试机制等功能。

## 📋 功能特性

- ✅ 任务入库持久化
- ✅ 基于 Kafka 的异步消息处理
- ✅ 业务键（businessKey）单调性判断
- ✅ 自动重试机制（最多3次）
- ✅ 任务状态跟踪（PENDING/SUCCESS/FAILED）
- ✅ 分布式追踪支持（traceId）

## 🏗️ 项目结构

```
src/main/java/com/example/notify/
├── NotifyApplication.java          # 启动类
├── config/
│   └── KafkaConfig.java            # Kafka配置
├── controller/
│   └── NotifyController.java       # 接入层
├── service/
│   └── NotifyService.java          # 服务层
├── consumer/
│   └── NotifyConsumer.java         # 消费层
├── entity/
│   └── NotifyTask.java             # 任务实体
├── repository/
│   └── NotifyTaskRepository.java   # 数据访问层
└── dto/
    ├── NotifyRequest.java          # 请求DTO
    └── NotifyResponse.java         # 响应DTO
```

## 🚀 快速开始

### 1. 前置要求

- JDK 17+
- Maven 3.6+
- Kafka 2.8+（需要先启动 Kafka 服务）

### 2. 启动 Kafka

```bash
# 使用 Docker 启动 Kafka（推荐）
docker-compose up -d

# 或使用本地安装的 Kafka
# 启动 Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# 启动 Kafka
bin/kafka-server-start.sh config/server.properties
```

### 3. 创建 Kafka Topic

```bash
# 创建主队列 Topic
kafka-topics.sh --create --topic notify-main-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 创建重试队列 Topic
kafka-topics.sh --create --topic notify-retry-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 4. 启动应用

```bash
mvn spring-boot:run
```

## 📡 API 接口

### 发送通知

```bash
POST /api/notify
Content-Type: application/json

{
  "targetUrl": "https://example.com/api/callback",
  "headers": {
    "Content-Type": "application/json",
    "Authorization": "Bearer token123"
  },
  "body": "{\"message\": \"test\"}",
  "businessKey": "order-123"
}
```

**响应：**

```json
{
  "taskId": "550e8400-e29b-41d4-a716-446655440000",
  "traceId": "trace-123",
  "message": "任务已接收"
}
```

### 查询任务状态

```bash
GET /api/notify/{taskId}
```

## 🔄 工作流程

1. **接收请求**：Controller 接收通知请求
2. **发送消息**：将任务发送到 Kafka 主队列（`notify-main-topic`）
3. **消费处理**：Consumer 从主队列消费消息
4. **顺序判断**：检查是否为同一 businessKey 的最新任务
5. **发送通知**：调用外部 API
6. **结果处理**：
   - 成功：更新状态为 SUCCESS
   - 失败：增加重试次数，发送到重试队列（`notify-retry-topic`）
7. **重试机制**：重试队列消费后转发到主队列，最多重试3次

## ⚙️ 配置说明

### Kafka 配置

在 `application.properties` 中配置：

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

### 数据库配置

使用 H2 内存数据库（开发环境），生产环境可替换为 MySQL/PostgreSQL：

```properties
spring.datasource.url=jdbc:h2:mem:notifydb
```

## 🔍 关键实现

### 1. 单调性判断

通过查询同一 `businessKey` 的最新任务ID，确保只处理最新任务：

```java
String latestTaskId = taskRepo.findLatestTaskIdByBusinessKey(task.getBusinessKey());
if (!task.getTaskId().equals(latestTaskId)) {
    // 非最新任务，跳过处理
    return;
}
```

### 2. 重试机制

失败任务自动发送到重试队列，重试队列消费后转发到主队列：

```java
if (task.getRetryCount() > 3) {
    task.setStatus("FAILED");
} else {
    kafkaTemplate.send("notify-retry-topic", task.getBusinessKey(), taskJson);
}
```

### 3. 手动确认

使用手动确认模式，确保消息处理完成后再确认：

```java
@KafkaListener(topics = "notify-main-topic", groupId = "notify-group")
public void consumeMainQueue(@Payload String message, Acknowledgment acknowledgment) {
    // 处理消息
    acknowledgment.acknowledge();
}
```

## 🧪 测试

### 使用 curl 测试

```bash
# 发送通知
curl -X POST http://localhost:8080/api/notify \
  -H "Content-Type: application/json" \
  -d '{
    "targetUrl": "https://httpbin.org/post",
    "headers": {
      "Content-Type": "application/json"
    },
    "body": "{\"test\": \"message\"}",
    "businessKey": "test-001"
  }'

# 查询任务状态
curl http://localhost:8080/api/notify/{taskId}
```

## 📊 监控

### H2 控制台

访问 `http://localhost:8080/h2-console` 查看数据库：

- JDBC URL: `jdbc:h2:mem:notifydb`
- Username: `sa`
- Password: (空)

### 查看 Kafka 消息

```bash
# 消费主队列消息
kafka-console-consumer.sh --topic notify-main-topic --bootstrap-server localhost:9092 --from-beginning

# 消费重试队列消息
kafka-console-consumer.sh --topic notify-retry-topic --bootstrap-server localhost:9092 --from-beginning
```

## 📝 注意事项

1. **Kafka 必须启动**：应用启动前需要先启动 Kafka
2. **Topic 需要创建**：首次运行需要创建 Topic
3. **手动确认**：使用手动确认模式，确保消息处理完成
4. **业务键设计**：`businessKey` 的设计要合理，确保同一业务使用相同的 key
5. **重试次数**：当前配置最多重试3次，可根据需求调整

## 🚀 生产环境建议

1. **数据库**：替换 H2 为 MySQL/PostgreSQL
2. **连接池**：配置数据库连接池
3. **监控**：集成 Prometheus + Grafana
4. **日志**：集成 ELK 或类似日志系统
5. **Kafka 集群**：使用 Kafka 集群提高可用性
6. **事务**：考虑使用 Kafka 事务保证数据一致性

