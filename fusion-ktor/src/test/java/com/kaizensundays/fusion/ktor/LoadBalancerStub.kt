package com.kaizensundays.fusion.ktor

import com.kaizensundays.fusion.messsaging.Instance
import com.kaizensundays.fusion.messsaging.LoadBalancer

/**
 * Created: Sunday 10/8/2023, 1:30 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class LoadBalancerStub : LoadBalancer {

    lateinit var instance: Instance

    override fun get() = instance

}