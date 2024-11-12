#!/bin/bash

# Ensuring the script is executed with root privileges

# Build and Ansible deploy
echo "Starting Build and Deployment Process..."

cd ansible
ENVIRONMENT=local
sudo ansible-playbook -i environments/local setup.yml

cd ..
sudo ./gradlew distDocker

cd ansible
sudo ansible-playbook -i environments/local couchdb.yml
sudo ansible-playbook -i environments/local initdb.yml
sudo ansible-playbook -i environments/local wipe.yml
sudo ansible-playbook -i environments/local apigateway.yml
sudo ansible-playbook -i environments/local openwhisk.yml
sudo ansible-playbook -i environments/local postdeploy.yml

# Append the OpenWhisk bin directory to the PATH in .bashrc
sudo echo 'export PATH=$PATH:~/InstaInfer-SoCC24/bin' | tee -a ~/.bashrc

# Reload .bashrc to update PATH
sudo  source ~/.bashrc

# Set wsk CLI properties
cd ..
wsk property set --apihost https://172.17.0.1:443
wsk property set --auth "$(cat ./ansible/files/auth.guest)"

docker pull suiyifan/squirrel:v1.3

# Create inference function actions
wsk action create ptest01 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i
wsk action create ptest04 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i
wsk action create ptest05 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i
wsk action create ptest06 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i
wsk action create ptest02 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i
wsk action create ptest03 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i
wsk action create ptest07 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i
wsk action create ptest08 --docker suiyifan/squirrel:v1.3 py2.py --memory 2048 -i

echo "Deployment completed successfully."