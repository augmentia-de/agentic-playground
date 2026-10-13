FROM eclipse-temurin:21-jre
WORKDIR /app
COPY agent-orchestrator/target/agent-orchestrator-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
