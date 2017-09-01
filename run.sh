#!/usr/bin/env bash

project=statistics

start() {
	status
	if [ ! $? -eq 0 ]; then
		echo "${project} is already running.."
		return 1
	fi

    nohup python ${project}.py > /dev/null 2>&1 &
    echo "${project} start success..."
}

stop() {
	ret=`status`
	if [ -z "${ret}" ]; then
	    echo "${project} not running.."
	    return 1
	fi

	kill -9 ${ret}

	status
	[ $? -eq 0 ] && echo "${project} stop success..." && return 1

	echo "${project} stop fail..."
	return 0
}

restart() {
    stop
    sleep 2
    start
}

status() {
    pid=`ps -ef | grep python | grep -v grep | grep ${project} | awk '{print $2}'`
    if [ -z ${pid} ]; then
        return 0
    fi
    echo "${pid}"
	return ${pid}
}

case "$1" in
	start|stop|restart|status)
  		$1
		;;
	*)
		echo $"Usage: $0 {start|stop|status|restart}"
		exit 1
esac
