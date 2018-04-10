#!/bin/bash
./gradlew clean build
sudo docker cp build/libs/com.niledb.nifi-0.8.0.nar nifi:/opt/nifi/nifi-1.6.0/lib
sudo docker restart nifi
#sudo docker logs -f nifi
