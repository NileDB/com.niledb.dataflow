#!/bin/bash
./gradlew clean build
sudo docker cp build/libs/com.niledb.dataflow-0.8.0.nar dataflow:/opt/nifi/nifi-1.6.0/lib
sudo docker restart dataflow
#sudo docker logs -f dataflow
