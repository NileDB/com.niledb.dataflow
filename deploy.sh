#!/bin/bash
./gradlew clean build
sudo docker cp build/libs/com.niledb.dataflow-0.9.0.nar dataflow:/opt/nifi/nifi-1.7.1/lib
sudo docker restart dataflow
#sudo docker logs -f dataflow
