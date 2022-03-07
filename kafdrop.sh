java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    -jar kafdrop-3.29.0.jar --server.port=9009 --management.server.port=9009 --kafka.brokerConnect="localhost:9092"