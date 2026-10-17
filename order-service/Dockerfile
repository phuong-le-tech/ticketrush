# Build depuis la racine du repo: docker build -f order-service/Dockerfile .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -B -q -DskipTests package -pl order-service -am

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
COPY --from=build /src/order-service/target/order-service-*-SNAPSHOT-exec.jar app.jar
USER 10001
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
