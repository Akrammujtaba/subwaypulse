# Build the job jar, then run it in Flink's application mode.
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY flink-jobs/pom.xml flink-jobs/pom.xml
RUN cd flink-jobs && mvn -q -B dependency:go-offline -DexcludeScope=test || true
COPY subwaypulse/schemas subwaypulse/schemas
COPY flink-jobs/src flink-jobs/src
RUN cd flink-jobs && mvn -q -B package -DskipTests

FROM flink:1.20.0-java17
# S3 filesystem for checkpoints, and the Prometheus metrics reporter (already a plugin).
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
 && cp /opt/flink/opt/flink-s3-fs-presto-1.20.0.jar /opt/flink/plugins/s3-fs-presto/
COPY --from=build /src/flink-jobs/target/subwaypulse-flink-0.1.0.jar /opt/flink/usrlib/subwaypulse.jar
