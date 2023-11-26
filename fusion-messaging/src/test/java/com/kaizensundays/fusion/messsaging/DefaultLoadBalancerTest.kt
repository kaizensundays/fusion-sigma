package com.kaizensundays.fusion.messsaging

import org.junit.Test
import reactor.test.StepVerifier

/**
 * Created: Saturday 10/21/2023, 12:16 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultLoadBalancerTest {

    @Test
    fun test() {

        var loadBalancer = DefaultLoadBalancer(emptyList())

        StepVerifier.create(loadBalancer.get())
            .expectError(IllegalStateException::class.java)
            .verify()

        loadBalancer = DefaultLoadBalancer(
            listOf(
                Instance("host0", 51000),
                Instance("host1", 51001),
                Instance("host2", 51002),
            )
        )

        listOf(
            Instance("host0", 51000),
            Instance("host1", 51001),
            Instance("host2", 51002),
            Instance("host0", 51000),
        )
            .forEach { expected ->
                StepVerifier.create(loadBalancer.get())
                    .expectNext(expected)
                    .verifyComplete()
            }
    }

}