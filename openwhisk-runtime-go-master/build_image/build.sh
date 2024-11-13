./tutorials/local_build.sh -r python3Action -t action-python-v3.7:1.0-SNAPSHOT
docker run -p 127.0.0.1:80:8080/tcp --name=bloom_whisker --rm -it action-python-v3.7:1.0-SNAPSHOT

