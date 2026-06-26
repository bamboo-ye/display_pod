ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-11
ARG FLINK_IMAGE=flink:1.18-java11

FROM ${MAVEN_IMAGE} AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM ${FLINK_IMAGE}
COPY --from=build /build/target/cvpr-flink-job-0.1.0.jar /opt/flink/usrlib/cvpr-flink-job.jar
