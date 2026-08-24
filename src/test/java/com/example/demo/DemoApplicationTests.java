package com.example.demo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

/**
 * The whole application context, wired to the real MySQL and Redis of
 * {@link TestcontainersConfiguration}.
 *
 * <p>This is not a formality. Everything else in the suite is a slice -- {@code @JdbcTest},
 * {@code @DataRedisTest}, {@code @WebMvcTest} -- and a slice cannot notice a bean that
 * fails to wire in the full context: an ambiguous {@code RedisScript<List>} injection, a
 * missing qualifier, a Lua script that is not on the classpath. This test does.
 *
 * <p>The {@code test} profile switches RocketMQ off ({@code rocketmq.enabled: false}), so
 * this also asserts what task 7 promised: the application starts with no broker, and the
 * {@code ObjectProvider<RateLimitEventPublisher>} in both services simply finds nothing.
 *
 * <p>The {@code RANDOM_PORT} web environment is not needed here, but it matches
 * {@link RateLimitEndToEndTest} exactly -- and identical test configuration is what lets
 * the two classes share one cached context, and with it one pair of containers. Leaving it
 * at the default {@code MOCK} would start MySQL and Redis a second time for this one
 * assertion.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class DemoApplicationTests {

	@Test
	@DisplayName("the application context starts against MySQL and Redis, with no broker")
	void contextLoads() {
	}

}
