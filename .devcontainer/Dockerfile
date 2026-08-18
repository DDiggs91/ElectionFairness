# Use the official Eclipse Temurin JDK 25 image on Ubuntu
FROM eclipse-temurin:25-jdk-noble

# Install basic development tools
RUN apt-get update && apt-get install -y \
    git \
    curl \
    xz-utils \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Maven manually (rather than via apt) so we don't pull in a second,
# conflicting JDK as a package dependency - we want Maven to run on the
# Temurin 25 JDK already in this image.
ARG MAVEN_VERSION=3.9.16
RUN curl -fsSL -o /tmp/maven.tar.gz \
    "https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
    && curl -fsSL -o /tmp/maven.tar.gz.sha512 \
    "https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz.sha512" \
    && echo "$(cat /tmp/maven.tar.gz.sha512)  /tmp/maven.tar.gz" | sha512sum -c - \
    && tar -xzf /tmp/maven.tar.gz -C /opt \
    && ln -s "/opt/apache-maven-${MAVEN_VERSION}" /opt/maven \
    && rm /tmp/maven.tar.gz /tmp/maven.tar.gz.sha512

ENV MAVEN_HOME=/opt/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"

# Set the default shell to bash
ENV SHELL=/bin/bash
