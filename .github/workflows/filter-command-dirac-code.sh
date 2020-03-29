#!/bin/sh

cd $(dirname "$0")

echo "[$(date)] $SSH_ORIGINAL_COMMAND" >> ssh-command.log

if [ "$SSH_ORIGINAL_COMMAND" != 'cd /var/www/html/ihep/tars && tar -xvf - && ls *.tar.gz > tars.list' ]; then
  echo "Command not allowed: $SSH_ORIGINAL_COMMAND"
  exit 1
fi

# Run the command
eval "$SSH_ORIGINAL_COMMAND"
