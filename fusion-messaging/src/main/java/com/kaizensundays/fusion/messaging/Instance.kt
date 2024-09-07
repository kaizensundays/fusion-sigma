package com.kaizensundays.fusion.messaging

/**
 * Created: Saturday 10/7/2023, 6:12 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
data class Instance(val protocol: String, val host: String, val port: Int) {

    constructor(host: String, port: Int) : this("http", host, port)

}