# ---- Stage 1: Maven cache & dependency resolution ----
FROM eclipse-temurin:21-jdk-alpine AS deps
WORKDIR /build
RUN apk add --no-cache maven bash curl > /dev/null 2>&1 || true

# Copy the dependency cache tarball from host's ~/.m2/repository/de/
COPY deps.tar.gz .

# Extract to Maven's local repository (~/.m2/repository/)
RUN mkdir -p /root/.m2/repository && \
    tar xzf deps.tar.gz -C /root/.m2/repository/ && \
    ls /root/.m2/repository/de/augmentia/quad/ && \
    rm deps.tar.gz

# Copy project pom and resolve additional dependencies  
COPY pom.xml .
RUN mvn dependency:resolve -q 2>/dev/null || echo "Some deps missing but will continue"

# ---- Stage 2: Compile and package ----
FROM deps AS builder
WORKDIR /app
COPY pom.xml .
COPY src/main/java ./src/main/java  
COPY src/main/resources ./src/main/resources
RUN mvn clean package -DskipTests -q 2>/dev/null || { \
    echo "Build failed with skip, retrying..."; \
    mvn package -DskipTests; \
}

# Verify build succeeded  
RUN test -f target/quad-workflow-worker-*.jar && echo "BUILD SUCCESS" || exit 1

# ---- Stage 3: Minimal runtime image ----
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy JAR from builder stage
COPY --from=builder /app/target/*.jar app.jar

# Workspace directory
RUN mkdir -p /var/lib/quad-worker/workspace && chown -R appuser:appgroup /var/lib/quad-worker

USER appuser

EXPOSE 8092

HEALTHCHECK --interval=15s --timeout=10s --retries=4 --start-period=90s \
    CMD wget -qO- http://localhost:8092/actuator/health || exit 1

ENTRYPOINT ["java", \
    "-XX:+UseG1GC", \
    "-Xms256m", \
    "-Xmx512m", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-Dspring.profiles.active=docker", \
    "-jar", "app.jar"]
