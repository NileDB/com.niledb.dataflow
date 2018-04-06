#!/bin/bash
gradle clean build
sudo docker cp build/libs/com.niledb.nifi-0.8.0.nar nifi:/opt/nifi/nifi-1.5.0/lib
sudo docker restart nifi
#sudo docker logs -f nifi
