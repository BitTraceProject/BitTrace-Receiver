#!/bin/bash

DEPLOY_PWD=$PWD
OUTPUT_DIR=$PWD/../output

function restart() {
  set -x
  cd $DEPLOY_PWD/.. || exit
  bash $DEPLOY_PWD/../build.sh

  # cp 会直接覆盖旧的
  echo "rebuild and restart:[receiver.receiver.bittrace.proj]"
  docker cp ${OUTPUT_DIR}/receiver-cli "receiver.receiver.bittrace.proj":/bittrace/
  docker restart "receiver.receiver.bittrace.proj"

  echo "rebuild and restart:[mq.receiver.bittrace.proj]"
  docker cp ${OUTPUT_DIR}/receiver-cli "mq.receiver.bittrace.proj":/bittrace/
  docker restart "mq.receiver.bittrace.proj"

  echo "rebuild and restart:[meta.receiver.bittrace.proj]"
  docker cp ${OUTPUT_DIR}/receiver-cli "meta.receiver.bittrace.proj":/bittrace/
  docker restart "meta.receiver.bittrace.proj"

  cd $DEPLOY_PWD || exit
}

restart
