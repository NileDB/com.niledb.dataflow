#!/bin/bash
gradle clean build
docker cp build/libs/com.niledb.nifi-0.8.0.nar nifi:/opt/nifi/nifi-1.5.0/lib
docker restart nifi
#docker logs -f nifi
