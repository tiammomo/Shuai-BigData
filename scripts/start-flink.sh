#!/bin/bash
# Flink 一键启动脚本

# 配置
FLINK_HOME=${FLINK_HOME:-"/opt/flink"}
DATA_DIR=${DATA_DIR:-"/data/flink"}
LOG_DIR=${LOG_DIR:-"/var/log/flink"}
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

# 检查 Flink 是否安装
check_flink() {
    if [ ! -d "$FLINK_HOME" ]; then
        log_error "Flink 未安装，请设置 FLINK_HOME 环境变量"
        exit 1
    fi
}

# 启动 JobManager
start_jobmanager() {
    log_info "启动 JobManager..."
    $FLINK_HOME/bin/jobmanager.sh start
    log_info "JobManager 启动完成"
}

# 启动 TaskManager
start_taskmanager() {
    local slots=${1:-4}
    log_info "启动 TaskManager (slots: $slots)..."

    # 配置 TaskManager
    cat > /tmp/flink-conf.yaml << EOF
taskmanager.numberOfTaskSlots: $slots
taskmanager.memory.process.size: 4G
taskmanager.memory.flink.size: 3G
taskmanager.rpc.port: 6122
EOF

    $FLINK_HOME/bin/taskmanager.sh start
    log_info "TaskManager 启动完成"
}

# 启动 Standalone 集群
start_cluster() {
    check_flink
    mkdir -p $DATA_DIR $LOG_DIR

    # 配置 flink-conf.yaml
    cat > $FLINK_HOME/conf/flink-conf.yaml << EOF
jobmanager.rpc.address: localhost
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 2G
jobmanager.memory.flink.size: 1.5G

taskmanager.numberOfTaskSlots: 4
taskmanager.memory.process.size: 4G
taskmanager.memory.flink.size: 3G

parallelism.default: 4

# Checkpoint 配置
execution.checkpointing.interval: 1min
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 10min
state.backend: rocksdb

# Web UI
rest.port: 8081
EOF

    # 配置 workers
    for i in $(seq 1 $CLUSTER_SIZE); do
        echo "localhost" >> $FLINK_HOME/conf/workers
    done

    start_jobmanager

    for i in $(seq 1 $CLUSTER_SIZE); do
        start_taskmanager 4
        sleep 2
    done

    log_info "Flink 集群启动完成"
}

# 停止 Flink
stop_flink() {
    log_info "停止 Flink..."
    $FLINK_HOME/bin/taskmanager.sh stop
    $FLINK_HOME/bin/jobmanager.sh stop
    log_info "Flink 已停止"
}

# 提交 Job
submit_job() {
    local jar=$1
    local class=${2:-""}
    local parallelism=${3:-4}
    local args=${4:-""}

    log_info "提交 Job: $jar"
    $FLINK_HOME/bin/flink run \
        -d \
        -p $parallelism \
        -c $class \
        $jar \
        $args
}

# 列出 Job
list_jobs() {
    log_info "运行中的 Job:"
    $FLINK_HOME/bin/flink list
}

# 取消 Job
cancel_job() {
    local job_id=$1
    log_info "取消 Job: $job_id"
    $FLINK_HOME/bin/flink cancel $job_id
}

# 查看 Job 详情
job_info() {
    local job_id=$1
    $FLINK_HOME/bin/flink info $job_id
}

# 主函数
main() {
    case "$1" in
        start)
            start_cluster
            ;;
        stop)
            stop_flink
            ;;
        restart)
            stop_flink
            sleep 3
            main start
            ;;
        submit)
            submit_job $2 $3 $4 "$5"
            ;;
        list)
            list_jobs
            ;;
        cancel)
            cancel_job $2
            ;;
        info)
            job_info $2
            ;;
        *)
            echo "用法: $0 {start|stop|restart|submit|list|cancel|info}"
            exit 1
            ;;
    esac
}

main "$@"
