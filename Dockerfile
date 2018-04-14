FROM apache/nifi:1.6.0

EXPOSE 8080 8080

COPY build/libs/com.niledb.dataflow-0.8.0.nar /opt/nifi/nifi-1.6.0/lib
