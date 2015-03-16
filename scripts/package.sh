#/bin/bash -e
# Copyright 2014 Treode, Inc.
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

SCRIPT_DIR=$(dirname $(readlink -f $0))

echo "PACKAGING TREODE FOR Debian..."

STAGEDIR0=/var/tmp/treode.$$
STAGEDIR=${STAGEDIR0}/treode-0.2.0
mkdir -p ${STAGEDIR}

cp ${SCRIPT_DIR}/conf/treode.init ${STAGEDIR}
cd ${STAGEDIR}

wget https://oss.treode.com/examples/finagle/0.2.0/finagle-server-0.2.0.jar

LOGNAME=treode@treode.com
DEBFULLNAME="Treode Inc"
dh_make -y -n -s -c apache -e treode@treode.com

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
fakeroot debian/rules binary

# If successful, copy .deb to $SCRIPT_DIR/o
if [ -e ${STAGEDIR}/../*.deb ]; then
   mkdir -p ${SCRIPT_DIR}/o
   mv ${STAGEDIR}/../*.deb ${SCRIPT_DIR}/o
   rm -rf ${STAGEDIR}
else
   echo "THERE WAS AN ERROR CREATING .DEB PACKAGE"
   echo "STAGE DIR ${STAGEDIR}"
fi

