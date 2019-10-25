## Segment Source

### Gunicorn

+ [Gunicorn Setup](https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-14-04)
+ allow for unlimited TCP connections: `ulimit -n 10240` 
+ `gunicorn --workers 10 --bind 0.0.0.0:5000 wsgi_conn_segment_source --daemon`  

### HAProxy

+ [HAProxy Setup](https://www.digitalocean.com/community/tutorials/how-to-use-haproxy-to-set-up-http-load-balancing-on-an-ubuntu-vps)
+ `service haproxy start`
  
## S3 Sink

+ Load AWS Credential in ~/.aws/credentials
+ `sudo python3 ingestor.py`
