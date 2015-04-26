#!/bin/bash

HOSTID=0x`hostname -i | sed -e 's/\./ /g' | xargs printf '%02X'`
CELLID=$HOSTID
HAIL=""
SOLO="-solo"
IP=`hostname -i`

echo args $*

service nginx start

while getopts "c:h:r:s:m" opt; do
   case "$opt" in
   h) HOSTID=${OPTARG}
      ;;
   c) CELLID=${OPTARG}
      ;;
   r) HAIL="-hail=${OPTARG}"
      SOLO=""
      ;;
   s) SHARE="-sharePeerAddr=${OPTARG}"
      SOLO=""
      ;;
   m) SOLO=""
      ;;
   esac
done

echo "Initializing movies - HOST ${HOSTID} (${IP}) - CELL ${CELLID}"
java -jar /var/lib/treode/lib/movies-server.jar init -host=${HOSTID} -cell=${CELLID} /var/lib/treode/db/store.3kv

echo "Starting movies."
echo "OPTIONS -> ${SOLO} ${SHARE} ${HAIL}"

java -jar /var/lib/treode/lib/movies-server.jar serve ${SOLO} ${SHARE} ${HAIL} /var/lib/treode/db/store.3kv  > /var/lib/treode/logs/server.log 2>&1 &

/bin/bash
