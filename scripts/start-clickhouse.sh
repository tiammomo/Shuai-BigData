#!/bin/bash
# ClickHouse 一键启动脚本

# 配置
CK_HOME=${CK_HOME:-"/opt/clickhouse"}
DATA_DIR=${DATA_DIR:-"/data/clickhouse"}
LOG_DIR=${LOG_DIR:-"/var/log/clickhouse"}
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

# 检查 ClickHouse 是否安装
check_clickhouse() {
    if [ ! -d "$CK_HOME" ]; then
        log_error "ClickHouse 未安装，请设置 CK_HOME 环境变量"
        exit 1
    fi
}

# 配置 ClickHouse
configure_clickhouse() {
    local port=$1
    local http_port=$2

    cat > $CK_HOME/etc/clickhouse-server/config.xml << EOF
<?xml version="1.0"?>
<clickhouse>
    <path>$DATA_DIR</path>
    <logger>
        <level>information</level>
        <log>$LOG_DIR/clickhouse.log</log>
        <errorlog>$LOG_DIR/clickhouse_err.log</log>
        <size>1000M</size>
        <count>3</count>
    </logger>
    <http_port>$http_port</http_port>
    <tcp_port>$port</tcp_port>
    <interserver_http_port>9009</interserver_http_port>
    <max_connections>4096</max_connections>
    <keep_alive_timeout>3</keep_alive_timeout>
    <max_concurrent_queries>100</max_concurrent_queries>
    <uncompressed_cache_size>8589934592</uncompressed_cache_size>
    <mark_cache_size>5368709120</mark_cache_size>
    <path>$DATA_DIR</path>
    <users_config>users.xml</users_config>
    <default_profile>default</default_profile>
    <network>
        <max_server_memory_usage_to_ram_ratio>0.8</max_server_memory_usage_to_ram_ratio>
    </network>
</clickhouse>
EOF
}

# 配置 Users
configure_users() {
    cat > $CK_HOME/etc/clickhouse-server/users.xml << EOF
<?xml version="1.0"?>
<clickhouse>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
            <allow_databases>
                <database>default</database>
            </allow_databases>
        </default>
        <root>
            <password>root</password>
            <profile>default</profile>
            <quota>default</quota>
        </root>
    </users>
    <profiles>
        <default>
            <max_memory_usage>32G</max_memory_usage>
            <max_execution_time>300</max_execution_time>
            <max_block_size>65536</max_block_size>
        </default>
    </profiles>
    <quotas>
        <default>
            <interval duration="3600">
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</result_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
EOF
}

# 启动 ClickHouse
start_clickhouse() {
    local port=${1:-9000}
    local http_port=${2:-8123}

    log_info "启动 ClickHouse (port: $port, http_port: $http_port)..."

    check_clickhouse
    mkdir -p $DATA_DIR $LOG_DIR
    configure_clickhouse $port $http_port
    configure_users

    $CK_HOME/bin/clickhouse-server \
        --config-file $CK_HOME/etc/clickhouse-server/config.xml \
        --daemon \
        --pid-file $DATA_DIR/clickhouse.pid

    sleep 3
    log_info "ClickHouse 启动完成"
}

# 启动集群
start_cluster() {
    check_clickhouse
    mkdir -p $DATA_DIR $LOG_DIR

    local base_port=9000
    local base_http=8123

    for i in $(seq 0 $((CLUSTER_SIZE-1))); do
        port=$((base_port + i))
        http_port=$((base_http + i))
        start_clickhouse $port $http_port
        sleep 2
    done

    log_info "ClickHouse 集群启动完成"
}

# 停止 ClickHouse
stop_clickhouse() {
    log_info "停止 ClickHouse..."
    if [ -f "$DATA_DIR/clickhouse.pid" ]; then
        kill $(cat $DATA_DIR/clickhouse.pid) 2>/dev/null
        rm -f $DATA_DIR/clickhouse.pid
    fi
    pkill -f clickhouse-server 2>/dev/null
    log_info "ClickHouse 已停止"
}

# 连接 ClickHouse
client() {
    local host=${1:-localhost}
    local port=${2:-8123}
    local user=${3:-root}
    local password=${4:-root}

    clickhouse-client \
        --host $host \
        --port $port \
        --user $user \
        --password $password
}

# 创建数据库
create_database() {
    local db_name=$1
    client -e "CREATE DATABASE IF NOT EXISTS $db_name;"
    log_info "数据库 $db_name 创建完成"
}

# 创建表
create_table() {
    local db_name=$1
    local table_sql=$2
    client -d $db_name -e "$table_sql"
    log_info "表创建完成"
}

# 查看状态
show_status() {
    log_info "ClickHouse 状态:"
    client -e "SELECT * FROM system.contacts FORMAT JSON;" 2>/dev/null
    echo ""
    client -e "SELECT * FROM system.databases FORMAT JSON;" 2>/dev/null
}

# 优化表
optimize_table() {
    local db_name=$1
    local table_name=$2
    log_info "优化表 $db_name.$table_name..."
    client -d $db_name -e "OPTIMIZE TABLE $table_name FINAL;"
    log_info "优化完成"
}

# 主函数
main() {
    case "$1" in
        start)
            start_cluster
            ;;
        start-single)
            start_clickhouse $2 $3
            ;;
        stop)
            stop_clickhouse
            ;;
        restart)
            stop_clickhouse
            sleep 3
            main start
            ;;
        client)
            client $2 $3 $4 $5
            ;;
        create-db)
            create_database $2
            ;;
        create-table)
            create_table $2 "$3"
            ;;
        status)
            show_status
            ;;
        optimize)
            optimize_table $2 $3
            ;;
        *)
            echo "用法: $0 {start|start-single|stop|restart|client|create-db|create-table|status|optimize}"
            exit 1
            ;;
    esac
}

main "$@"
