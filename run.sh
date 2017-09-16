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

all() {
	status
	if [ ! $? -eq 0 ]; then
		echo "${project} is already running.."
		return 1
	fi

    nohup python ${project}.py -w all > /dev/null 2>&1 &
    echo "${project} start success..."
}

stop() {
	status
	if [ $? -eq 0 ]; then
	    echo "${project} not running.."
	    return 1
	fi

	ps -ef | grep python | grep -v grep | grep ${project} | awk '{print $2}' | xargs kill -9

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
    if [ -z "${pid}" ]; then
        return 0
    fi
    echo "${pid}"
    return 1
}

case "$1" in
	all|start|stop|restart|status)
  		$1
		;;
	*)
		echo $"Usage: $0 {all|start|stop|status|restart}"
		exit 1
esac
