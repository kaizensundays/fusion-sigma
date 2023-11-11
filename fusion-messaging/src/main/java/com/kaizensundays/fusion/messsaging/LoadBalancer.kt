package com.kaizensundays.fusion.messsaging

/**
 * Created: Sunday 10/8/2023, 1:24 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
fun interface LoadBalancer {

    fun get(): Instance

}