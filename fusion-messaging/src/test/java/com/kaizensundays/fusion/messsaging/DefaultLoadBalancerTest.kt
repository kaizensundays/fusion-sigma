package com.kaizensundays.fusion.messaging

import com.kaizensundays.fusion.messsaging.DefaultLoadBalancer
import com.kaizensundays.fusion.messsaging.Instance
import org.junit.Test
import kotlin.test.assertEquals

/**
 * Created: Saturday 10/21/2023, 12:16 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultLoadBalancerTest {

    @Test
    fun test() {

        val loadBalancer = DefaultLoadBalancer(
            listOf(
                Instance("host0", 51000),
                Instance("host1", 51001),
                Instance("host2", 51002),
            )
        )

        assertEquals(Instance("host0", 51000), loadBalancer.get())
        assertEquals(Instance("host1", 51001), loadBalancer.get())
        assertEquals(Instance("host2", 51002), loadBalancer.get())
        assertEquals(Instance("host0", 51000), loadBalancer.get())
    }

}