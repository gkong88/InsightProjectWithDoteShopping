
Instructions for Ubuntu 18.04 AMI
1) Install python3 pip package installer
https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-programming-environment-on-ubuntu-18-04-quickstart

TODO: Load AWS Credential in ~/.aws/credentials


Ref: https://github.com/confluentinc/confluent-kafka-python
Ref: https://github.com/segment-integrations/connect-kafka


apt-get install libapache2-mod-wsgi

pip3 install confluent-kafka
pip3 install flask
pip3 install flask_restful
pip3 install flatten_json
pip3 install typing


USAGE: 
gunicorn --workers 4 --bind 0.0.0.0:5000 wsgi_conn_segment_source --daemon

# allow for unlimited TCP connections.i
ulimit -n 10240


sudo python3 ingestor.py &
disown <process id> 


s3 sink dependencies:


Dependencies:

pip3 install smart_open

pip3 install kafka

pip3 install pytz

~/.aws/config

[default]
region=us-west-2


~/.aws/credentials
[default]
.... 
