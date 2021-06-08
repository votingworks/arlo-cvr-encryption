# set-up script for a fresh Ubuntu node; run this with sudo -H
apt update

# install Python 
dpkg --configure -a
add-apt-repository ppa:deadsnakes/ppa -y
apt update
# apt upgrade
apt install --assume-yes make python3-pip python3.8 libgmp-dev libmpfr-dev libmpc-dev python3.8-dev
apt install --assume-yes python3.8-distutils htop
# apt update libcurl4

wget https://bootstrap.pypa.io/get-pip.py
python3.8 get-pip.py
pip install --upgrade setuptools

rm -f /usr/bin/python /usr/bin/python3
ln -s /usr/bin/python3.8 /usr/bin/python3
ln -s /usr/bin/python3.8 /usr/bin/python

pip install six==1.16.0 requests==2.25.1  # solves some dependency issues
pip install -e 'git+https://github.com/votingworks/electionguard-python.git#egg=electionguard'
pip install -e 'git+https://github.com/votingworks/arlo-cvr-encryption#egg=arlo-cvr-encryption'

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
