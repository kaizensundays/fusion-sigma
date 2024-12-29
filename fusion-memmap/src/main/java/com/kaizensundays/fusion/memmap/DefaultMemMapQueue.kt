package com.kaizensundays.fusion.memmap

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.time.Duration

/**
 * Created: Saturday 1/20/2024, 6:45 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@SuppressWarnings(
    "kotlin:S6518", // get()
)
class DefaultMemMapQueue(
    private val baseDir: String,
    private val queueName: String,
    private val maxQueueSize: Int
) : ReactiveQueue<ByteArray> {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val MESSAGE_META_SIZE = 4
        const val QUEUE_META_SIZE = 8

        const val HEAD_BYTE_LOC = 0
        const val TAIL_BYTE_LOC = 4
    }

    init {
        File(baseDir).mkdirs()
    }

    private val mode = "rw"

    private val fileName = baseDir + File.separatorChar + queueName

    private val dataFile = RandomAccessFile("$fileName.data", mode)
    private val metaFile = RandomAccessFile("$fileName.meta", mode)

    private val dataBuffer = dataFile.channel.map(FileChannel.MapMode.READ_WRITE, 0, maxQueueSize.toLong())
    private val metaBuffer = metaFile.channel.map(FileChannel.MapMode.READ_WRITE, 0, QUEUE_META_SIZE.toLong())

    private fun head(): Int {
        return metaBuffer.getInt(HEAD_BYTE_LOC)
    }

    private fun head(loc: Int) {
        metaBuffer.putInt(HEAD_BYTE_LOC, loc)
    }

    private fun tail(): Int {
        return metaBuffer.getInt(TAIL_BYTE_LOC)
    }

    private fun tail(loc: Int) {
        metaBuffer.putInt(TAIL_BYTE_LOC, loc)
    }

    private fun isEmpty(): Boolean = head() == tail()

    private fun isFull(messageSize: Int): Boolean {
        val head = head()
        val tail = tail()
        val totalMessageSize = messageSize + MESSAGE_META_SIZE
        val dataSize = when {
            (head < tail) -> {
                tail - head
            }

            (head > tail) -> {
                tail + (maxQueueSize - head)
            }

            else -> {
                0
            }
        }
        val freeSpace = maxQueueSize - dataSize
        return if ((tail + totalMessageSize) % maxQueueSize == head) {
            true
        } else {
            totalMessageSize > freeSpace
        }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun doOffer(data: ByteArray, timeout: Duration): Mono<Boolean> {

        return if (isFull(data.size)) {
            Mono.just(false)
        } else {
            val tail = tail()
            dataBuffer.position(tail)
            if (tail + MESSAGE_META_SIZE + data.size >= maxQueueSize) {
                val messageSizeData = ByteBuffer.allocate(MESSAGE_META_SIZE).putInt(data.size).array()
                val allData = messageSizeData + data
                allData.mapIndexed { index, byte ->
                    dataBuffer.put((tail + index) % maxQueueSize, byte)
                }
            } else {
                dataBuffer.putInt(data.size)
                dataBuffer.put(data)
            }
            tail((tail + MESSAGE_META_SIZE + data.size) % maxQueueSize)

            Mono.just(true)
        }

    }

    override fun offer(data: ByteArray, timeout: Duration): Mono<Boolean> {
        return try {
            doOffer(data, timeout)
        } catch (e: Exception) {
            Mono.error(e)
        }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun doPoll(timeout: Duration): Mono<ByteArray> {

        return if (isEmpty()) {
            Mono.empty()
        } else {

            val head = head()
            dataBuffer.position(head)
            val messageSize = if (head + MESSAGE_META_SIZE > maxQueueSize) {
                val messageSizeBytes = (0 until MESSAGE_META_SIZE).map { index ->
                    dataBuffer.get((head + index) % maxQueueSize)
                }.toByteArray()
                ByteBuffer.wrap(messageSizeBytes).int
            } else {
                dataBuffer.int
            }
            val data: ByteArray =
                if (head + MESSAGE_META_SIZE + messageSize > maxQueueSize) {
                    (0 until messageSize).fold(byteArrayOf()) { acc, index ->
                        acc + dataBuffer.get(
                            (head + MESSAGE_META_SIZE + index) % maxQueueSize
                        )
                    }
                } else {
                    val messageByteArray = ByteArray(messageSize)
                    dataBuffer.get(messageByteArray)
                    messageByteArray
                }
            head((head + MESSAGE_META_SIZE + messageSize) % maxQueueSize)

            Mono.just(data)
        }

    }

    override fun poll(timeout: Duration): Mono<ByteArray> {
        return try {
            doPoll(timeout)
        } catch (e: Exception) {
            Mono.error(e)
        }
    }

}