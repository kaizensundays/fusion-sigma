package com.kaizensundays.fusion.okhttp

import com.kaizensundays.fusion.messaging.Producer
import okhttp3.OkHttpClient
import okhttp3.Request
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI

/**
 * Created: Sunday 10/1/2023, 1:36 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class OkHttpProducer : Producer {

    override fun request(topic: URI, messages: Flux<ByteArray>): Flux<ByteArray> {
        return Flux.empty()
    }

    override fun request(topic: URI, msg: ByteArray): Flux<ByteArray> {

        val client = OkHttpClient.Builder().build()

        val request = Request.Builder().url("http://localhost:7701/ping").build()

        val response = client.newCall(request).execute()

        println(response)

        return Flux.empty()
    }

    override fun request(topic: URI): Flux<ByteArray> {
        return request(topic, byteArrayOf())
    }

    override fun send(topic: URI, msg: ByteArray): Mono<Void> {
        return Mono.empty()
    }
}