#!/bin/bash
# 大数据组件一键启动脚本

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

# 检查组件是否安装
check_component() {
    local name=$1
    local env_var=$2
    local home=${!env_var}

    if [ -z "$home" ] || [ ! -d "$home" ]; then
        log_warn "$name 未安装 (需要设置 $env_var)"
        return 1
    fi
    log_info "$name 已安装: $home"
    return 0
}

# 启动 ZooKeeper
start_zookeeper() {
    if [ -n "$KAFKA_HOME" ] && [ -d "$KAFKA_HOME" ]; then
        log_info "启动 ZooKeeper..."
        $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon \
            $KAFKA_HOME/config/zookeeper.properties
        sleep 3
    fi
}

# 启动 Kafka
start_kafka() {
    if [ -n "$KAFKA_HOME" ] && [ -d "$KAFKA_HOME" ]; then
        log_info "启动 Kafka..."
        $KAFKA_HOME/bin/kafka-server-start.sh -daemon \
            $KAFKA_HOME/config/server.properties
    fi
}

# 启动 Flink
start_flink() {
    if [ -n "$FLINK_HOME" ] && [ -d "$FLINK_HOME" ]; then
        log_info "启动 Flink..."
        $FLINK_HOME/bin/start-cluster.sh
    fi
}

# 启动 Doris FE
start_doris_fe() {
    if [ -n "$DORIS_HOME" ] && [ -d "$DORIS_HOME" ]; then
        log_info "启动 Doris FE..."
        $DORIS_HOME/fe/bin/start_fe.sh --daemon
    fi
}

# 启动 Doris BE
start_doris_be() {
    if [ -n "$DORIS_HOME" ] && [ -d "$DORIS_HOME" ]; then
        log_info "启动 Doris BE..."
        $DORIS_HOME/be/bin/start_be.sh --daemon
    fi
}

# 启动 ClickHouse
start_clickhouse() {
    if [ -n "$CK_HOME" ] && [ -d "$CK_HOME" ]; then
        log_info "启动 ClickHouse..."
        $CK_HOME/bin/clickhouse-server --config-file \
            $CK_HOME/etc/clickhouse-server/config.xml --daemon
    fi
}

# 启动 MySQL
start_mysql() {
    if command -v mysqld &> /dev/null; then
        log_info "启动 MySQL..."
        mysqld --user=mysql --daemon
    fi
}

# 启动 Redis
start_redis() {
    if command -v redis-server &> /dev/null; then
        log_info "启动 Redis..."
        redis-server --daemonize yes
    fi
}

# 启动 Elasticsearch
start_elasticsearch() {
    if [ -n "$ES_HOME" ] && [ -d "$ES_HOME" ]; then
        log_info "启动 Elasticsearch..."
        $ES_HOME/bin/elasticsearch -d
    fi
}

# 停止所有组件
stop_all() {
    log_info "停止所有组件..."

    # 停止 Flink
    if [ -n "$FLINK_HOME" ] && [ -d "$FLINK_HOME" ]; then
        $FLINK_HOME/bin/stop-cluster.sh 2>/dev/null
    fi

    # 停止 Kafka
    if [ -n "$KAFKA_HOME" ] && [ -d "$KAFKA_HOME" ]; then
        $KAFKA_HOME/bin/kafka-server-stop.sh 2>/dev/null
        $KAFKA_HOME/bin/zookeeper-server-stop.sh 2>/dev/null
    fi

    # 停止 ClickHouse
    pkill -f clickhouse-server 2>/dev/null

    # 停止 Redis
    redis-cli shutdown 2>/dev/null

    log_info "所有组件已停止"
}

# 查看状态
status() {
    log_info "组件状态:"

    echo ""
    echo "=== Java 进程 ==="
    ps aux | grep -E "kafka|flink|doris|clickhouse" | grep -v grep | head -20

    echo ""
    echo "=== 端口监听 ==="
    netstat -tlnp 2>/dev/null | grep -E "9092|8081|8030|9050|9000|8123|3306|6379|9200" || \
    ss -tlnp 2>/dev/null | grep -E "9092|8081|8030|9050|9000|8123|3306|6379|9200"
}

# 健康检查
health_check() {
    log_info "健康检查:"

    local all_ok=true

    # 检查 Kafka
    if [ -n "$KAFKA_HOME" ]; then
        if $KAFKA_HOME/bin/kafka-broker-api-versions.sh \
            --bootstrap-server localhost:9092 &>/dev/null; then
            log_info "  [OK] Kafka: localhost:9092"
        else
            log_warn "  [WARN] Kafka: 未响应"
            all_ok=false
        fi
    fi

    # 检查 Flink
    if [ -n "$FLINK_HOME" ]; then
        if curl -s http://localhost:8081/overview &>/dev/null; then
            log_info "  [OK] Flink: localhost:8081"
        else
            log_warn "  [WARN] Flink: 未响应"
            all_ok=false
        fi
    fi

    # 检查 Doris
    if [ -n "$DORIS_HOME" ]; then
        if mysql -h localhost -P 9030 -u root -e "SHOW FRONTENDS;" &>/dev/null; then
            log_info "  [OK] Doris FE: localhost:9030"
        else
            log_warn "  [WARN] Doris FE: 未响应"
            all_ok=false
        fi
    fi

    # 检查 ClickHouse
    if [ -n "$CK_HOME" ]; then
        if curl -s http://localhost:8123/ &>/dev/null; then
            log_info "  [OK] ClickHouse: localhost:8123"
        else
            log_warn "  [WARN] ClickHouse: 未响应"
            all_ok=false
        fi
    fi

    # 检查 Redis
    if command -v redis-cli &> /dev/null; then
        if redis-cli ping &>/dev/null; then
            log_info "  [OK] Redis: localhost:6379"
        else
            log_warn "  [WARN] Redis: 未响应"
            all_ok=false
        fi
    fi

    if $all_ok; then
        log_info "所有组件健康!"
    fi
}

# 主函数
main() {
    echo "======================================"
    echo "  大数据组件管理脚本"
    echo "======================================"

    case "$1" in
        start)
            check_component "Kafka" "KAFKA_HOME"
            check_component "Flink" "FLINK_HOME"
            check_component "Doris" "DORIS_HOME"
            check_component "ClickHouse" "CK_HOME"

            start_mysql
            start_redis
            start_zookeeper
            start_kafka
            start_flink
            start_doris_fe
            sleep 5
            start_doris_be
            start_clickhouse
            start_elasticsearch

            echo ""
            health_check
            ;;
        stop)
            stop_all
            ;;
        restart)
            stop_all
            sleep 3
            main start
            ;;
        status)
            status
            ;;
        health)
            health_check
            ;;
        check)
            check_component "Kafka" "KAFKA_HOME"
            check_component "Flink" "FLINK_HOME"
            check_component "Doris" "DORIS_HOME"
            check_component "ClickHouse" "CK_HOME"
            ;;
        *)
            echo "用法: $0 {start|stop|restart|status|health|check}"
            echo ""
            echo "环境变量:"
            echo "  KAFKA_HOME - Kafka 安装目录"
            echo "  FLINK_HOME - Flink 安装目录"
            echo "  DORIS_HOME - Doris 安装目录"
            echo "  CK_HOME    - ClickHouse 安装目录"
            echo "  ES_HOME    - Elasticsearch 安装目录"
            exit 1
            ;;
    esac
}

main "$@"
