set -ex

OS=$(uname -s)
ARCH=$(uname -m)


yum install -y zip flex bison gcc-gfortran
curl "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH.zip" -o "awscliv2.zip"
unzip -qq awscliv2.zip
./aws/install
