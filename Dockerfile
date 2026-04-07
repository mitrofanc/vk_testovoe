FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /build

COPY pom.xml ./
RUN mvn -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -DskipTests package dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/dependency

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=build /build/target/classes ./classes
COPY --from=build /build/target/dependency ./libs

EXPOSE 9090

ENTRYPOINT ["java", "-cp", "/app/classes:/app/libs/*", "vk.kvstore.app.GrpcServerApplication"]
