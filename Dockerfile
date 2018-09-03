FROM apache/nifi:1.7.1

EXPOSE 8080 8080

COPY build/libs/com.niledb.dataflow-0.9.0.nar /opt/nifi/nifi-1.7.1/lib
