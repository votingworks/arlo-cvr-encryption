# set-up script for a fresh Ubuntu node; run this with sudo -H
apt update

# install Python 
wget https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh || true
bash Anaconda3-5.0.1-Linux-x86_64.sh -b -p $HOME/anaconda3 || true
echo 'export PATH="$HOME/anaconda3/bin:$PATH"' >> ~/.bashrc
. ~/.bashrc
conda uninstall -y blaze
conda install -y python=3.8 anaconda=custom
pip install -U pip
pip install boto3==1.15.12  # newer than what's in the AMI
# - sudo -H pip install -e 'git+https://github.com/rkooo567/arlo-e2e-noop#egg=arlo_e2e_noop'
git clone https://github.com/danwallach/arlo-e2e-noop.git
pip install -e arlo-e2e-noop
#    - sudo -H pip install -U 'ray==1.0.0'
pip install https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-1.1.0.dev0-cp37-cp37m-manylinux1_x86_64.whl
apt install htop
apt-get install -y pkg-config
apt install make

# install GCC (https://gist.github.com/jlblancoc/99521194aba975286c80f93e47966dc5)
apt-get install -y software-properties-common python-software-properties
apt-get install -y software-properties-common
add-apt-repository ppa:ubuntu-toolchain-r/test
apt update
apt install g++-7 -y
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 60 \
                         --slave /usr/bin/g++ g++ /usr/bin/g++-7 
update-alternatives --config gcc

# echo 'export PATH="$HOME/anaconda3/envs/tensorflow_p36/bin:$PATH"' >> ~/.bashrc
# pkill -9 apt-get || true
# pkill -9 dpkg || true
# dpkg --configure -a
# add-apt-repository ppa:deadsnakes/ppa -y
# apt update
# apt install --assume-yes make python3-pip python3.8 libgmp-dev libmpfr-dev libmpc-dev python3.8-dev
# apt install s3fs
# apt update libcurl4
# rm -f /bin/python
# ln -s /usr/bin/python3.8 /bin/python
# python -m pip install --upgrade pip
# pip install virtualenv==20.0.23 pipenv
# # pip install boto3==1.4.8  # 1.4.8 adds InstanceMarketOptions
# pip install boto3==1.15.12
pip install -e 'git+https://github.com/microsoft/electionguard-python.git@feature/generic_chaum_petersen#egg=electionguard'
# pip install -e 'git+https://github.com/votingworks/arlo-e2e#egg=arlo_e2e'
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-1.1.0.dev0-cp38-cp38-manylinux1_x86_64.whl
# #pip install ray==1.0.0rc2

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
