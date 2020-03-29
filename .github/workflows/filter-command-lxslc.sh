#!/bin/bash

cd $(dirname "$0")

secret_file=enc-secret

validate_mod() {
  mod=$(stat -c %a $1)
  if [ "${mod: -2}" != '00' ]; then
    echo "Access permission for '$1' not correct, execute 'chmod 600 $1'"
    return 1
  fi
}

validate_mod "$secret_file" || exit 1

eval "cmds=($SSH_ORIGINAL_COMMAND)"

if [ ${#cmds[@]} -ne 2 ]; then
  echo "Command format not correct: $SSH_ORIGINAL_COMMAND"
  exit 1
fi


remote_command=${cmds[1]}

# Only allow specified commands
case $remote_command in
  'cvmfs_server transaction dcomputing.ihep.ac.cn || true')
    ;;
  'tar -C / -xf -')
    ;;
  'cvmfs_server publish dcomputing.ihep.ac.cn')
    ;;
  *)
    echo "Remote command not allowed: $remote_command"
    exit 1
    ;;
esac


# Decrypt the password
pass_enc=${cmds[0]#'PASS_ENC='}

pass_origin=$(echo "$pass_enc" | openssl enc -aes-256-cbc -d -base64 -kfile "$secret_file" -md sha512)

if [ $? -ne 0 ]; then
  echo 'Pass decryption failed!!!'
  exit 1
fi


# Connect to the remote server
export SSHPASS=$pass_origin

/cvmfs/dcomputing.ihep.ac.cn/hpc/sw/x86_64-sl6/sshpass/1.06/bin/sshpass -e ssh dcomputingpub@cvmfs-stratum-zero.ihep.ac.cn "$remote_command"
