ARG SPARK_TAG=3.5.8
FROM apache/spark:${SPARK_TAG}

USER root

# Download S3A dependencies (hadoop-aws and aws-java-sdk-bundle)
# These are required for History Server to read event logs from S3/MinIO
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Set Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
