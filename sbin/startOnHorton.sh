#!/usr/bin/env bash

readonly sbin="$( cd "$( dirname $0 )" && pwd )"

init() {
	source "$sbin/config.sh"
	source "$sbin/utils.sh"
}

main() {
	init
	local d=0
	local c=""
	while getopts "dc" arg
	do
		case $arg in
			d)
				let d=1
				;;
			c)
				c=$c"-c"
				;;
			?)
				;;
		esac
	done
	if [ $d -eq 1 ]
	then
		local_job_submit $c
	else
		horton_job_submit $c
	fi
	exit 0
}

main $@
