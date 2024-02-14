FROM mtr.devops.telekom.de/tardis-internal/pandora/pandora-java-21

WORKDIR app

COPY build/libs/polaris.jar app.jar

EXPOSE 8080

CMD ["java", "-jar", "app.jar"]