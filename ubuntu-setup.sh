# set-up script for a fresh Ubuntu node; run this with sudo -H
apt update

# install Python 
dpkg --configure -a
add-apt-repository ppa:deadsnakes/ppa -y
apt update
# apt upgrade
apt install --assume-yes make python3-pip python3.8 libgmp-dev libmpfr-dev libmpc-dev python3.8-dev
apt install --assume-yes python3.8-distutils s3fs htop
# apt update libcurl4

wget https://bootstrap.pypa.io/get-pip.py
python3.8 get-pip.py
pip install --upgrade setuptools

rm -f /usr/bin/python /usr/bin/python3
ln -s /usr/bin/python3.8 /usr/bin/python3
ln -s /usr/bin/python3.8 /usr/bin/python

pip install boto3==1.15.12  # newer than what's in the AMI
pip install six==1.13.0 requests==2.18.0  # solves some dependency issues
# pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-1.1.0.dev0-cp38-cp38-manylinux1_x86_64.whl
pip install ray==1.0.0
pip install -e 'git+https://github.com/microsoft/electionguard-python.git@feature/generic_chaum_petersen#egg=electionguard'
pip install -e 'git+https://github.com/votingworks/arlo-e2e#egg=arlo_e2e'

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
