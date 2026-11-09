# JVM workload image: the JDK compiles Server.java at BUILD time so the running
# process carries no compiler state. Built once on the source VM and `docker save`d to the
# destination (identical image ID: CRIU restore needs the same rootfs on both ends).
FROM eclipse-temurin:21-jdk-jammy
COPY Server.java /app/Server.java
RUN javac -d /app /app/Server.java
