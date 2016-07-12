#!/bin/bash

function getTimestamp(){
    local curTime=$1
    local todayNum=`date -d ${curTime} +%s`
    echo ${todayNum}
}

function getEarlyTimestamp(){
    local todayTime=$(getTimestamp $1)
    # 518400=6*24*3600
    local sixDays=$((518400))
    local targetTime=$(($todayTime-sixDays))
    echo ${targetTime}
}

function findTimeJustBefore(){
    local ret=0
    local targetTime=$1
    local filePath=$2

# ls $filePath is going to be change according to hdfs.
    for file in `ls ${filePath}`
    do
        if [ -d "${filePath}/${file}" ]
        then
            timeStr=` echo ${file}|sed 's/.*\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\).*/\1/g' `
            time=`date -d ${timeStr} +%s`
            if [[ ${time} -lt ${targetTime} ]];
            then
                if [[ ${ret} -eq 0 ]]
                then
                    ret=${time}
                else
                    if [[ ${ret} -lt ${time} ]]
                    then
                        ret=${time}
                    fi
                fi
            fi
        fi
    done
    if [[ ${ret} -eq 0 ]]
    then
        echo "none"
    else
        retStr=`date -d @${ret} +%Y-%m-%d`
#        echo $filePath/$retStr
        echo ${retStr}
    fi
}

if [[ $# -lt 2 ]];
then
    echo "Usage:"
    echo "  $0 date_for_today path"
    exit 1
else

    targetTimestamp=$(getTimestamp $1)
    folder=$2
    findTimeJustBefore ${targetTimestamp} ${folder}
fi
