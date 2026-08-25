# --- build ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -e -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

# --- run ---
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/fhir-converter.jar /app/fhir-converter.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/fhir-converter.jar"]
