#!/bin/bash
set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
LOG_DIR=$DIR/logs
STATE_FILE=$DIR/state_file_`date +%Y%M%d_%H%M%S`
PID_FILE=$DIR/pid_file_`date +%Y%M%d_%H%M%S`

mkdir -p $LOG_DIR
luigid --background --pidfile $PID_FILE --logdir $LOG_DIR --state-path $STATE_FILE
