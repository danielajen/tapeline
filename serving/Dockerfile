# Spring Boot on a JRE base, built in layers.
#
# Build context is the REPOSITORY ROOT, not this directory:
#
#   docker build -f serving/Dockerfile -t tapeline-serving .
#
# The serving tier compiles its gRPC stubs from proto/, which sits outside
# this module. A Docker build cannot reach above its context, so the context
# has to be the root — the alternative, duplicating the .proto files into
# each module, is exactly the drift the single proto directory prevents.
#
# The layered extraction below matters more than it looks. A Spring Boot fat
# jar is mostly dependencies that change rarely plus a few hundred KB of
# application classes that change every commit. Copying the jar as one layer
# means every deploy pushes the whole thing; extracting it into dependency
# and application layers means a normal deploy pushes only the small one.

FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Dependencies first, as their own cached layer.
COPY serving/pom.xml ./pom.xml
RUN mvn -B -q dependency:go-offline

COPY proto /proto
COPY serving/src ./src

# Tests run inside the build, so an image cannot be produced from code that
# does not pass them.
RUN mvn -B package -Dprotobuf.source.root=/proto

RUN java -Djarmode=layertools -jar target/tapeline-serving-0.1.0.jar extract --destination /layers

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S tapeline && adduser -S tapeline -G tapeline
USER tapeline
WORKDIR /app

# Least-changing layer first, so Docker's cache is actually useful.
COPY --from=build /layers/dependencies/ ./
COPY --from=build /layers/spring-boot-loader/ ./
COPY --from=build /layers/snapshot-dependencies/ ./
COPY --from=build /layers/application/ ./

EXPOSE 8080 9090

# MaxRAMPercentage rather than a fixed -Xmx: the JVM reads the container
# limit, so the heap tracks whatever the pod is given instead of being wrong
# every time that limit changes.
#
# ExitOnOutOfMemoryError is deliberate. A JVM that has exhausted its heap is
# not going to recover; a pod that dies gets restarted, while one that
# thrashes serves timeouts indefinitely and looks healthy to a liveness probe.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
