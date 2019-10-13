
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

# Segment Ingestion

gunicorn and wsgi setup: https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-14-04
haproxy:     https://www.digitalocean.com/community/tutorials/how-to-use-haproxy-to-set-up-http-load-balancing-on-an-ubuntu-vps

USAGE: 
gunicorn --workers 4 --bind 0.0.0.0:5000 wsgi_conn_segment_source --daemon

# S3

TODO: Load AWS Credential in ~/.aws/credentials

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
