package com.example.demo;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The infrastructure every {@code @SpringBootTest} in this project runs against: a real
 * MySQL and a real Redis, supplied as beans rather than through {@code @Testcontainers}.
 *
 * <p>Declaring the containers as beans is what lets Spring's context cache do the work.
 * Every test class that imports this configuration shares one application context, so the
 * two containers are started once for the whole run instead of once per class -- with
 * static {@code @Container} fields each class would pay the startup cost again.
 *
 * <p>{@code @ServiceConnection} derives {@code spring.datasource.*} and
 * {@code spring.data.redis.*} from the running containers, which is why
 * {@code application-test.yaml} deliberately hardcodes neither. Redis needs the explicit
 * {@code name = "redis"} because a bare {@link GenericContainer} carries no image hint the
 * connection detail factory could match on.
 *
 * <p>The MySQL settings mirror {@code docker-compose.yaml} exactly -- server time zone
 * {@code +08:00} and {@code connectionTimeZone=+08:00} on the JDBC URL, the {@code +}
 * URL-encoded as {@code %2B} because {@code withUrlParam} appends the value verbatim and
 * Connector/J would read a bare {@code +} as a space. Testing against a different time zone
 * configuration from the one that ships would prove nothing about it.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    @SuppressWarnings("resource")
    MySQLContainer<?> mysqlContainer() {
        return new MySQLContainer<>("mysql:8.0")
                .withCommand("--default-time-zone=+08:00")
                .withUrlParam("connectionTimeZone", "%2B08:00")
                // The very same init.sql the compose file mounts; see the testResource
                // entry in pom.xml that puts it on the test classpath.
                .withInitScript("init.sql");
    }

    @Bean
    @ServiceConnection(name = "redis")
    @SuppressWarnings("resource")
    GenericContainer<?> redisContainer() {
        return new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);
    }
}
