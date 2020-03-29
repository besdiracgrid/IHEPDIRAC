# Release and Deploy

Use [GitHub Actions](https://github.com/features/actions)
to release and deploy IHEPDIRAC client.


## Self-hosted runner

We use
[self-hosted](https://help.github.com/en/actions/hosting-your-own-runners/about-self-hosted-runners)
runner because we need to access the release
and deployment server, which is not easy to do on GitHub-hosted runners.

Create a CentOS 7 virtual machine for self-hosted runner.


### Add user for runner

Create user `ihepdirac`.

```shell
$ useradd -m ihepdirac
$ passwd ihepdirac
```

Allow `ihepdirac` to use `sudo`.

```shell
$ usermod -aG wheel ihepdirac
```


### Install runner

[Install the runner](https://help.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners).

[Configure as a service](https://help.github.com/en/actions/hosting-your-own-runners/configuring-the-self-hosted-runner-application-as-a-service).

```shell
$ sudo ./svc.sh install
$ sudo ./svc.sh start
$ sudo ./svc.sh status
```


### Docker

Install docker because we need to install `IHEPDIRAC` on SL6.

[Install latest docker](https://docs.docker.com/install/linux/docker-ce/centos/).

Allow `ihepdirac` to create containers.

```shell
$ sudo usermod -aG docker ihepdirac
```

The docker image
[ihepdirac/sl6-ihepdirac-base](https://hub.docker.com/repository/docker/ihepdirac/sl6-ihepdirac-base)
is created for running the action. The source could be found on
<https://github.com/besdiracgrid/ihep-docker>.


## Workflow file

The workflow file is `.github/workflows/release-deploy.yml`.
Pushing a version tag will trigger this action.

```shell
$ git push origin v0r1
```


### Secrets

The following secrets should be
[added in GitHub settings](https://help.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets):

1. `IHEPDIRAC_KEY`. This is the private ssh key, and the corresponding
   public key will be added to `dirac-code` and `lxslc6`.
2. `TEMP_PROXY`. A temporary proxy which is needed for
   `dirac-configure`.
3. `MID_HOST`. The lxslc6 login name, should be like
   `user@lxslc6.ihep.ac.cn`.
4. `CVMFS_PASS_ENC`. Encrypted cvmfs password.


## dirac-code server

Add the public ssh key in `~/.ssh/authorized_keys` for user `www`.
Use the following options:

```
command="~/.ssh/filter-command-dirac-code.sh",from="IP_ADDRESS",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty ssh-rsa XXXXX ihepdirac@ihepdirac-runner
```

Copy `filter-command-dirac-code.sh` file. Change the `IP_ADDRESS` to the
host IP of the virtual machine.

The `command` will only allow limited commands to run.


## lxslc6 server

Add the public ssh key in `~/.ssh/authorized_keys` for your user.

```
command="~/deploy-ihepdirac/filter-command-lxslc.sh",from="IP_ADDRESS",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty ssh-rsa XXXXX ihepdirac@ihepdirac-runner
```

Copy the file `filter-command-lxslc.sh` to lxslc6 server.


### Generate encrypted password

Create `enc-secret` file in the same directory
of `filter-command-lxslc.sh`:

```shell
$ head /dev/urandom | tr -dc A-Za-z0-9 | head -c 64 > enc-secret
$ chmod 600 enc-secret
```

Generate the encrypted password with the above secret:

```shell
$ openssl enc -aes-256-cbc -base64 -kfile enc-secret -md sha512 -salt
```

Press `Ctrl+D` to exit. Save the encrypted result as `secrets.CVMFS_PASS_ENC`.
