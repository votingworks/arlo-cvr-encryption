# set-up script for a fresh Ubuntu node; run this with sudo -H
echo 'export PATH="$HOME/anaconda3/envs/tensorflow_p36/bin:$PATH"' >> ~/.bashrc
pkill -9 apt-get || true
pkill -9 dpkg || true
dpkg --configure -a
add-apt-repository ppa:deadsnakes/ppa -y
apt update
apt install --assume-yes make python3-pip python3.8 libgmp-dev libmpfr-dev libmpc-dev python3.8-dev
apt install s3fs
apt update libcurl4
rm -f /bin/python
ln -s /usr/bin/python3.8 /bin/python
python -m pip install --upgrade pip
pip install virtualenv==20.0.23 pipenv
pip install boto3==1.4.8  # 1.4.8 adds InstanceMarketOptions
pip install -e 'git+https://github.com/microsoft/electionguard-python.git@feature/generic_chaum_petersen#egg=electionguard'
pip install -e 'git+https://github.com/votingworks/arlo-e2e#egg=arlo_e2e'
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-1.1.0.dev0-cp38-cp38-manylinux1_x86_64.whl
#pip install ray==1.0.0rc2

# we need *lots* of open files to make this work, for some reason (or, we need many more VMs with fewer cores)
sysctl -w fs.file-max=500000
# recommendations per http://www.brendangregg.com/blog/2017-12-31/reinvent-netflix-ec2-tuning.html
sysctl -w net.core.somaxconn=1000
sysctl -w net.core.netdev_max_backlog=5000
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
sysctl -w 'net.ipv4.tcp_wmem=4096 12582912 16777216'
sysctl -w 'net.ipv4.tcp_rmem=4096 12582912 16777216'
sysctl -w net.ipv4.tcp_max_syn_backlog=8096
sysctl -w net.ipv4.tcp_slow_start_after_idle=0
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w 'net.ipv4.ip_local_port_range=10240 65535'
sysctl -w net.ipv4.tcp_abort_on_overflow=1

mkdir /mnt/arlo-data