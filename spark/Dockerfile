FROM apache/spark:3.5.0

USER root

ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# -------------------------
# Install curl properly (Debian-based image)
# -------------------------
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# -------------------------
# Add Hadoop AWS jars
# -------------------------
RUN curl -L -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

USER spark