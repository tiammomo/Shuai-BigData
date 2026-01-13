#!/bin/bash
# Kafka 一键启动脚本

# 配置
KAFKA_HOME=${KAFKA_HOME:-"/opt/kafka"}
DATA_DIR=${DATA_DIR:-"/data/kafka"}
LOG_DIR=${LOG_DIR:-"/var/log/kafka"}
CLUSTER_SIZE=${CLUSTER_SIZE:-3}

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查 Kafka 是否安装
check_kafka() {
    if [ ! -d "$KAFKA_HOME" ]; then
        log_error "Kafka 未安装，请设置 KAFKA_HOME 环境变量"
        exit 1
    fi
}

# 启动 ZooKeeper
start_zookeeper() {
    log_info "启动 ZooKeeper..."
    $KAFKA_HOME/bin/zookeeper-server-start.sh \
        -daemon \
        $KAFKA_HOME/config/zookeeper.properties
    sleep 3
    log_info "ZooKeeper 启动完成"
}

# 启动 Kafka Broker
start_broker() {
    local id=$1
    log_info "启动 Kafka Broker $id..."

    # 生成配置文件
    cat > /tmp/server-$id.properties << EOF
broker.id=$id
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:909$id
log.dirs=$DATA_DIR/broker-$id
log.retention.hours=168
log.segment.bytes=1073741824
num.network.threads=8
num.io.threads=32
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=6000
default.replication.factor=3
min.insync.replicas=2
auto.create.topics.enable=true
delete.topic.enable=true
EOF

    $KAFKA_HOME/bin/kafka-server-start.sh \
        -daemon /tmp/server-$id.properties
}

# 停止 Kafka
stop_kafka() {
    log_info "停止 Kafka..."
    $KAFKA_HOME/bin/kafka-server-stop.sh
    $KAFKA_HOME/bin/zookeeper-server-stop.sh
    log_info "Kafka 已停止"
}

# 创建 Topic
create_topic() {
    local topic=$1
    local partitions=${2:-3}
    local replication=${3:-3}

    log_info "创建 Topic: $topic"
    $KAFKA_HOME/bin/kafka-topics.sh \
        --create \
        --topic $topic \
        --bootstrap-server localhost:9092 \
        --partitions $partitions \
        --replication-factor $replication
}

# 查看 Topic 列表
list_topics() {
    log_info "Topic 列表:"
    $KAFKA_HOME/bin/kafka-topics.sh \
        --list \
        --bootstrap-server localhost:9092
}

# 查看 Topic 详情
describe_topic() {
    local topic=$1
    $KAFKA_HOME/bin/kafka-topics.sh \
        --describe \
        --topic $topic \
        --bootstrap-server localhost:9092
}

# 主函数
main() {
    case "$1" in
        start)
            check_kafka
            mkdir -p $DATA_DIR $LOG_DIR
            start_zookeeper
            sleep 5
            for i in $(seq 0 $((CLUSTER_SIZE-1))); do
                start_broker $i
                sleep 2
            done
            log_info "Kafka 集群启动完成"
            ;;
        stop)
            stop_kafka
            ;;
        restart)
            stop_kafka
            sleep 3
            main start
            ;;
        create-topic)
            create_topic $2 $3 $4
            ;;
        list-topics)
            list_topics
            ;;
        describe-topic)
            describe_topic $2
            ;;
        *)
            echo "用法: $0 {start|stop|restart|create-topic|list-topics|describe-topic}"
            exit 1
            ;;
    esac
}

main "$@"
