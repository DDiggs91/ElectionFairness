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

# Node for puerto-rico-frontend's local dev loop (npm run dev/test:unit) and Playwright.
# ./mvnw verify itself doesn't need this — frontend-maven-plugin installs its own isolated
# Node (pre-warmed below) — but a system install makes the day-to-day npm workflow usable
# directly.
ARG NODE_VERSION=22.22.0
RUN cd /tmp \
    && curl -fsSL -O "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
    && curl -fsSL -O "https://nodejs.org/dist/v${NODE_VERSION}/SHASUMS256.txt" \
    && grep " node-v${NODE_VERSION}-linux-x64.tar.xz\$" SHASUMS256.txt | sha256sum -c - \
    && tar -xJf "node-v${NODE_VERSION}-linux-x64.tar.xz" -C /opt \
    && ln -s "/opt/node-v${NODE_VERSION}-linux-x64" /opt/node \
    && rm "node-v${NODE_VERSION}-linux-x64.tar.xz" SHASUMS256.txt

ENV PATH="/opt/node/bin:${PATH}"

# Pre-warm frontend-maven-plugin's own isolated Node/npm install (see
# puerto-rico-frontend/pom.xml) so a fresh container doesn't pay for the
# download+extract interactively on the first `./mvnw verify`. Only the two
# pom.xml files needed to resolve the frontend module's parent are copied in
# here — the rest of the source is bind-mounted at runtime, not baked into
# the image. Runs as ubuntu so ${user.home} matches what the real build
# resolves at container run time.
USER ubuntu
WORKDIR /home/ubuntu/frontend-node-prewarm
COPY --chown=ubuntu:ubuntu pom.xml pom.xml
COPY --chown=ubuntu:ubuntu puerto-rico-frontend/pom.xml puerto-rico-frontend/pom.xml
RUN mvn -f puerto-rico-frontend/pom.xml com.github.eirslett:frontend-maven-plugin:install-node-and-npm
WORKDIR /home/ubuntu
RUN rm -rf /home/ubuntu/frontend-node-prewarm
USER root

# Set the default shell to bash
ENV SHELL=/bin/bash
