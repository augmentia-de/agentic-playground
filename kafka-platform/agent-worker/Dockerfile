FROM eclipse-temurin:21-jre
WORKDIR /app
COPY agent-worker/target/agent-worker-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
