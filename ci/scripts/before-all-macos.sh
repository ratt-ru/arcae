set -ex

brew install python llvm
brew reinstall gcc  # Need for gfortran
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /
