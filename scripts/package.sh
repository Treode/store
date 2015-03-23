#/bin/bash -e
# Copyright 2015 Treode, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

create_debian_package () {
    $PKGVER=$1
    $WGETFILE=$2
    $JARFILE=$3
    $TREODE_STAGEDIR=$4

    STAGEDIR0=/var/tmp/treode.$$
    STAGEDIR=${STAGEDIR0}/treode-${PKGVER}
    mkdir -p ${STAGEDIR}

    cp ${SCRIPT_DIR}/conf/treode.init ${STAGEDIR}
    cd ${STAGEDIR}

    if [[ $WGETFILE == 1 ]]; then
        wget https://oss.treode.com/examples/finagle/0.2.0/finagle-server-0.2.0.jar
    else
	echo COPY $JARFILE to `pwd`
        cp $JARFILE .
    fi

    LOGNAME=questions@treode.com
    DEBFULLNAME="Treode Inc"
    dh_make -y -n -s -c apache -e ${LOGNAME}

    # Customize debian control files
    cat ${SCRIPT_DIR}/conf/treode.rules >> debian/rules
    sed -i -e '/^Depends:/ s/$/, \${treode:Depends}/' debian/control
    sed -i -e 's/^Homepage:/Homepage: http:\/\/www.treode.com/' debian/control
    sed -i -e '/^Description:/{N;s/.*/Description: The DB that\x27s replicated, sharded and transactional.\
 TreodeDB is an open-source NoSQL database that shards for scalability, replicates for \
 reliability, and yet provides full ACID transactions.\
 TreodeDB connects to Spark for analytics, and it integrates well with CDNs for speed.\
 TreodeDB lets engineers develop the application, rather than work around the data architecture./}' debian/control

    export DESTDIR=`pwd`/debian
    fakeroot debian/rules clean
    PKGVER="${PKGVER}" fakeroot debian/rules binary

    # If successful, copy .deb to $SCRIPT_DIR/o
    if [ -e ${STAGEDIR}/../*.deb ]; then
        mv ${STAGEDIR}/../*.deb ${TREODE_STAGEDIR}
        rm -rf ${STAGEDIR}
    else
        echo "THERE WAS AN ERROR CREATING .DEB PACKAGE"
        echo "STAGE DIR ${STAGEDIR}"
    fi

}

PKGTYPE=
PKGVER=
JARFILE=
TREODE_STAGEDIR=
WGETFILE=0
while getopts "t:V:j:o:" OPTION
do
    case $OPTION in
        t)
            PKGTYPE=$OPTARG
            ;;
        V)
            PKGVER=$OPTARG
            ;;
        j)
            JARFILE=`readlink -f $OPTARG`
            ;;
        o)
            TREODE_STAGEDIR=`readlink -f $OPTARG`
            ;;
    esac
done

if [[ -z $PKGTYPE ]] || [[ -z $PKGVER ]] || [[ -z $TREODE_STAGEDIR ]] ; then
    echo "Please provide package type and version using -t and -V options"
    exit 1
fi

if [[ -z $JARFILE ]] ; then
    WGETFILE=1
    JARFILE="https://oss.treode.com/examples/finagle/${PKGVER}/finagle-server-${PKGVER}.jar"
    echo "Using $JARFILE"
fi

SCRIPT_DIR=$(dirname $(readlink -f $0))

case $PKGTYPE in
    debian)
        echo "PACKAGING TREODE FOR Debian..."
        create_debian_package $PKGVER $WGETFILE $JARFILE $TREODE_STAGEDIR
        ;;
    ?)
        echo "PACKAGING TREODE FOR $PKGTYPE is not implemented"
        exit 1
        ;;
esac

