
Instructions for Ubuntu 18.04 AMI
1) Install python3 pip package installer
https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-programming-environment-on-ubuntu-18-04-quickstart


Ref: https://github.com/confluentinc/confluent-kafka-python
Ref: https://github.com/segment-integrations/connect-kafka



pip3 install confluent-kafka
pip3 install "confluent-kafka[avro]"
pip3 install --no-binary :all: confluent-kafka
