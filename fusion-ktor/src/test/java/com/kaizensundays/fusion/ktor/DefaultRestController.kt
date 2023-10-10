package com.kaizensundays.fusion.ktor

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

/**
 * Created: Saturday 10/7/2023, 1:33 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@RestController
class DefaultRestController {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @GetMapping("/ping")
    fun ping(): String {
        return "Ok"
    }

    @PostMapping("/submit")
    fun submit(@RequestBody bytes: ByteArray): String {
        logger.info("submit: {}", String(bytes))
        return "Ok"
    }

}