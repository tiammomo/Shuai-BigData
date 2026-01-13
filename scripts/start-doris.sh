#!/bin/bash
# Doris 一键启动脚本

# 配置
DORIS_HOME=${DORIS_HOME:-"/opt/doris"}
DATA_DIR=${DATA_DIR:-"/data/doris"}
LOG_DIR=${LOG_DIR:-"/var/log/doris"}
FE_NODES=${FE_NODES:-3}
BE_NODES=${BE_NODES:-3}

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

# 检查 Doris 是否安装
check_doris() {
    if [ ! -d "$DORIS_HOME" ]; then
        log_error "Doris 未安装，请设置 DORIS_HOME 环境变量"
        exit 1
    fi
}

# 配置 FE
configure_fe() {
    local fe_id=$1
    local port=$2

    cat > $DORIS_HOME/fe/conf/fe.conf << EOF
priority_networks = 0.0.0.0/24
http_port = $((port + 100))
rpc_port = $((port + 200))
query_port = $((port + 300))
edit_log_port = $((port + 400))

meta_dir = $DATA_DIR/fe/meta
log_dir = $LOG_DIR/fe

JAVA_HOME = /usr/lib/jvm/java-11-openjdk-amd64
JAVA_OPTS = "-Xmx4096m -XX:+UseG1GC"

FE_ID = $fe_id
EOF
}

# 配置 BE
configure_be() {
    local be_id=$1
    local port=$2

    cat > $DORIS_HOME/be/conf/be.conf << EOF
priority_networks = 0.0.0.0/24
be_port = $port
webserver_port = $((port + 100))
heartbeat_service_port = $((port + 200))
brpc_port = $((port + 300))

storage_root_path = $DATA_DIR/be/storage
log_dir = $LOG_DIR/be

JAVA_HOME = /usr/lib/jvm/java-11-openjdk-amd64
JAVA_OPTS = "-Xmx8192m -XX:+UseG1GC"

be_node_num_per_disk = 2
EOF
}

# 启动 FE
start_fe() {
    local fe_id=$1
    local port=$2
    log_info "启动 FE $fe_id..."

    configure_fe $fe_id $port
    mkdir -p $DATA_DIR/fe/meta-$fe_id

    $DORIS_HOME/fe/bin/start_fe.sh --daemon --helper localhost:$((port + 400))
}

# 启动 BE
start_be() {
    local be_id=$1
    local port=$2
    log_info "启动 BE $be_id..."

    configure_be $be_id $port
    mkdir -p $DATA_DIR/be/storage-$be_id

    $DORIS_HOME/be/bin/start_be.sh --daemon
}

# 添加 BE 到集群
add_be_to_cluster() {
    local be_host=$1
    local be_port=$2

    mysql -h localhost -P 9030 -u root << EOF
ALTER SYSTEM ADD BACKEND "$be_host:$be_port";
EOF
}

# 启动集群
start_cluster() {
    check_doris
    mkdir -p $DATA_DIR $LOG_DIR

    local fe_base_port=8030
    local be_base_port=9050

    # 启动 FE
    for i in $(seq 1 $FE_NODES); do
        start_fe $i $((fe_base_port + i - 1))
        sleep 3
    done

    log_info "等待 FE 选举完成..."
    sleep 10

    # 启动 BE
    for i in $(seq 1 $BE_NODES); do
        start_be $i $((be_base_port + i - 1))
        sleep 2
    done

    log_info "添加 BE 到集群..."
    for i in $(seq 1 $BE_NODES); do
        add_be_to_cluster "localhost" $((be_base_port + i - 1))
    done

    log_info "Doris 集群启动完成"
}

# 停止 Doris
stop_doris() {
    log_info "停止 Doris..."

    # 停止 BE
    for pid in $(ps aux | grep 'be.conf' | grep -v grep | awk '{print $2}'); do
        kill $pid 2>/dev/null
    done

    # 停止 FE
    for pid in $(ps aux | grep 'fe.conf' | grep -v grep | awk '{print $2}'); do
        kill $pid 2>/dev/null
    done

    log_info "Doris 已停止"
}

# 查看状态
show_status() {
    log_info "FE 状态:"
    mysql -h localhost -P 9030 -u root -e "SHOW FRONTENDS;" 2>/dev/null

    echo ""
    log_info "BE 状态:"
    mysql -h localhost -P 9030 -u root -e "SHOW BACKENDS;" 2>/dev/null
}

# 创建数据库
create_database() {
    local db_name=$1
    mysql -h localhost -P 9030 -u root -e "CREATE DATABASE IF NOT EXISTS $db_name;"
    log_info "数据库 $db_name 创建完成"
}

# 创建表
create_table() {
    local db_name=$1
    local table_sql=$2
    mysql -h localhost -P 9030 -u root $db_name -e "$table_sql"
    log_info "表创建完成"
}

# 主函数
main() {
    case "$1" in
        start)
            start_cluster
            ;;
        stop)
            stop_doris
            ;;
        restart)
            stop_doris
            sleep 3
            main start
            ;;
        status)
            show_status
            ;;
        create-db)
            create_database $2
            ;;
        create-table)
            create_table $2 "$3"
            ;;
        add-be)
            add_be_to_cluster $2 $3
            ;;
        *)
            echo "用法: $0 {start|stop|restart|status|create-db|create-table|add-be}"
            exit 1
            ;;
    esac
}

main "$@"
