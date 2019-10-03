
Instructions for Ubuntu 18.04 AMI
1) Install python3 pip package installer
https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-programming-environment-on-ubuntu-18-04-quickstart


Ref: https://github.com/confluentinc/confluent-kafka-python
Ref: https://github.com/segment-integrations/connect-kafka


apt-get install libapache2-mod-wsgi

pip3 install confluent-kafka
pip3 install flask
pip3 install flask_restful
pip3 install flatten_json
pip3 install typing


USAGE: 
gunicorn --bind 0.0.0.0:8000 wsgi
