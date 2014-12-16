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

#
# Common Variables
#

: ${LOG:=$DIR/build.log}

: ${GRUNT:='grunt'}
: ${NPM:='npm'}
: ${SBT:=${DIR}'/scripts/sbt'}

#
# Common Functions
#

# Exit the shell if the actual exit status of the most recent command is not the expected value.
# If an error occurs, echo a message to both $LOG and STDOUT. The default expected value is 0, as
# most command line utilities exit with status zero on success. A handful of commands do something
# different.
#
# Usage:
#     expect-status [expected] [message]
expect-status() {
  local status=$?
  local log=${LOG:-"build.log"}
  local expected=${1:-0}
  local message=${2:-"Failed"}
  if [ $status -ne $expected ] ; then
    echo $message
    echo $message >> $log
    exit 1
  fi
}

# Echo the command to $LOG and STDOUT, and then do it. If the command exits with a non-zero status
# exit this shell.
#
# This is somewhat like supplying the `-xe` options to bash. However, the `-x` option for bash is
# too noisy for the regular build, and we do not want the `-e` option applied to every command.
#
# Usage:
#    echo-do command args...
echo-do() {
  local log=${LOG:-"build.log"}
  echo $*
  echo $* >> $log
  $* >> $log 2>&1
  expect-status 0 "$1 failed"
}

# Echo a message to $LOG and STDOUT.
#
# Usage:
#     log message...
log() {
  local log=${LOG:-"build.log"}
  echo $*
  echo $* >> $log
}

# Really clean everything.
#
# Usage:
#    clean
clean() {
  if [ -z "$SKIP_CLEAN" ] ; then
    echo-do git clean -dfx
  fi
}

# Double check the log file for failures. Exit with a parting message.
#
# Usage:
#    warpup
wrapup() {
  # Sometimes tests fail and yet SBT exits with a good status.
  egrep 'ABORTED|FAILED' build.log
  expect-status 1 "Failures found in build.log"
  echo "Build successful"
  exit 0
}
