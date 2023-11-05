package com.kaizensundays.fusion.messsaging

import java.util.concurrent.atomic.AtomicLong

/**
 * Created: Saturday 10/14/2023, 7:33 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultLoadBalancer(private val instances: List<Instance>) : LoadBalancer {

    private val index = AtomicLong()

    override fun get(): Instance {
        val idx = index.getAndIncrement()
        return instances[(idx % instances.size).toInt()]
    }

}