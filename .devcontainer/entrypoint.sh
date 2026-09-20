#!/usr/bin/env bash
# Entry point of the dev service. The container runs as the host user's uid,
# which may not exist in the image; give it a passwd entry and a writable HOME
# so git, sbt and Conan behave, then run the requested command.
set -eu

home="${HOME:-/home/dev}"
if [ ! -d "${home}" ]; then
  mkdir -p "${home}"
fi

if ! whoami >/dev/null 2>&1; then
  if [ -w /etc/passwd ]; then
    echo "dev:x:$(id -u):$(id -g):spark-milvus developer:${home}:/bin/bash" >> /etc/passwd
  fi
  if [ -w /etc/group ] && ! getent group "$(id -g)" >/dev/null 2>&1; then
    echo "dev:x:$(id -g):" >> /etc/group
  fi
fi

exec "$@"
