/*
 * Copyright 2023-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.krpc.compression.zstd

import kotlinx.coroutines.test.runTest
import kotlinx.rpc.krpc.KrpcConfigBuilder
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ZstdKrpcMessageCodecTest {
    @Test
    fun `round trips compressed payload`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val original = "hello ".repeat(4096).encodeToByteArray()

        val compressed = codec.compress(original)
        val decompressed = codec.decompress(compressed, maxDecompressedSize = original.size)

        assertTrue(compressed.size < original.size)
        assertContentEquals(original, decompressed)
    }

    @Test
    fun `rejects decompressed payload over limit`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val original = "hello ".repeat(4096).encodeToByteArray()
        val compressed = codec.compress(original)

        assertFailsWith<IllegalArgumentException> {
            codec.decompress(compressed, maxDecompressedSize = original.size - 1)
        }
    }

    @Test
    fun `round trips payload across compression input chunks`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val original = "hello ".repeat(64 * 1024).encodeToByteArray()

        val compressed = codec.compress(original)
        val decompressed = codec.decompress(compressed, maxDecompressedSize = original.size)

        assertContentEquals(original, decompressed)
    }

    @Test
    fun `recovers after a failed decompression`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val original = "hello ".repeat(4096).encodeToByteArray()
        val compressed = codec.compress(original)

        assertFailsWith<IllegalArgumentException> {
            codec.decompress(compressed, maxDecompressedSize = 16)
        }

        assertContentEquals(original, codec.decompress(compressed, maxDecompressedSize = original.size))
    }

    @Test
    fun `fails on corrupted input`() = runTest {
        val codec = ZstdKrpcMessageCodec()

        assertFailsWith<IllegalArgumentException> {
            codec.decompress(byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8), maxDecompressedSize = 1024)
        }
    }

    @Test
    fun `rejects frames whose window exceeds the supported limit`() = runTest {
        val codec = ZstdKrpcMessageCodec()

        val exception = assertFailsWith<IllegalArgumentException> {
            codec.decompress(HIGH_WINDOW_EMPTY_FRAME, maxDecompressedSize = 64 * 1024 * 1024)
        }

        assertContains(exception.message.orEmpty(), "window")
    }

    @Test
    fun `rejects concatenated frames`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val compressed = codec.compress("hello".encodeToByteArray())

        val exception = assertFailsWith<IllegalArgumentException> {
            codec.decompress(compressed + compressed, maxDecompressedSize = 1024)
        }

        assertContains(exception.message.orEmpty(), "exactly one frame")
    }

    @Test
    fun `accepts every frame content size layout`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val frames = listOf(
            ByteArray(0) to zstdFrame(0x00, 0x00, 0x01, 0x00, 0x00),
            ByteArray(0) to zstdFrame(0x20, 0x00, 0x01, 0x00, 0x00),
            ByteArray(256) to (
                zstdFrame(0x40, 0x00, 0x00, 0x00, 0x01, 0x08, 0x00) + ByteArray(256)
            ),
            ByteArray(0) to zstdFrame(0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00),
            ByteArray(0) to zstdFrame(
                0xc0, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01, 0x00, 0x00,
            ),
        )

        frames.forEach { (expected, frame) ->
            assertContentEquals(expected, codec.decompress(frame, maxDecompressedSize = 1024))
        }
    }

    @Test
    fun `accepts dictionary id checksum and rle fields`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val frames = listOf(
            ByteArray(0) to zstdFrame(0x01, 0x00, 0x00, 0x01, 0x00, 0x00),
            ByteArray(0) to zstdFrame(0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00),
            ByteArray(0) to zstdFrame(0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00),
            ByteArray(0) to zstdFrame(
                0x04, 0x00, 0x01, 0x00, 0x00,
                0x99, 0xe9, 0xd8, 0x51, // low 32 bits of the empty-content checksum
            ),
            "aaaa".encodeToByteArray() to zstdFrame(0x00, 0x00, 0x23, 0x00, 0x00, 'a'.code),
        )

        frames.forEach { (expected, frame) ->
            assertContentEquals(expected, codec.decompress(frame, maxDecompressedSize = 1024))
        }
    }

    @Test
    fun `rejects reserved frame and block values`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val invalidFrames = listOf(
            zstdFrame(0x08, 0x00, 0x01, 0x00, 0x00),
            zstdFrame(0x00, 0x00, 0x07, 0x00, 0x00),
            zstdFrame(0x00, 0x68, 0x09, 0x00, 0x10), // 128 KiB + 1 raw block
        )

        invalidFrames.forEach { frame ->
            assertFailsWith<IllegalArgumentException> {
                codec.decompress(frame, maxDecompressedSize = 1024)
            }
        }
    }

    @Test
    fun `rejects every truncated frame field`() = runTest {
        val codec = ZstdKrpcMessageCodec()
        val truncatedFrames = listOf(
            ByteArray(0),
            zstdFrame(0x00),
            zstdFrame(0x01, 0x00),
            zstdFrame(0x40, 0x00, 0x00),
            zstdFrame(0x00, 0x00, 0x01, 0x00),
            zstdFrame(0x00, 0x00, 0x11, 0x00, 0x00, 0x01),
            zstdFrame(0x04, 0x00, 0x01, 0x00, 0x00, 0x99, 0xe9, 0xd8),
        )

        truncatedFrames.forEach { frame ->
            val exception = assertFailsWith<IllegalArgumentException> {
                codec.decompress(frame, maxDecompressedSize = 1024)
            }
            assertContains(exception.message.orEmpty(), "incomplete")
        }
    }

    @Test
    fun `rejects frames with too many blocks`() = runTest {
        val codec = ZstdKrpcMessageCodec()

        val exception = assertFailsWith<IllegalArgumentException> {
            codec.decompress(frameWithTooManyBlocks(), maxDecompressedSize = 1024)
        }

        assertContains(exception.message.orEmpty(), "too many blocks")
    }

    @Test
    fun `round trips empty payload`() = runTest {
        val codec = ZstdKrpcMessageCodec()

        val compressed = codec.compress(ByteArray(0))
        val decompressed = codec.decompress(compressed, maxDecompressedSize = 1)

        assertContentEquals(ByteArray(0), decompressed)
    }

    @Test
    fun `configures connector compression through DSL`() {
        val connector = KrpcConfigBuilder.Connector()

        connector.zstd(minimumMessageSize = 10, maxDecompressedMessageSize = 20)

        val compression = assertNotNull(connector.messageCompression)
        assertIs<ZstdKrpcMessageCodec>(compression.codec)
        assertEquals(10, compression.minimumMessageSize)
        assertEquals(20, compression.maxDecompressedMessageSize)
    }
}

private val HIGH_WINDOW_EMPTY_FRAME = byteArrayOf(
    0x28, 0xb5.toByte(), 0x2f, 0xfd.toByte(), // zstd magic
    0x00, // no content size, dictionary, or checksum
    0x88.toByte(), // 128 MiB window
    0x01, 0x00, 0x00, // empty last raw block
)

private val ZSTD_MAGIC = byteArrayOf(0x28, 0xb5.toByte(), 0x2f, 0xfd.toByte())

private fun zstdFrame(vararg bytes: Int): ByteArray {
    return ZSTD_MAGIC + ByteArray(bytes.size) { bytes[it].toByte() }
}

private fun frameWithTooManyBlocks(): ByteArray {
    val frameHeaderSize = ZSTD_MAGIC.size + 2
    val frame = ByteArray(frameHeaderSize + 65_536 * 3)
    ZSTD_MAGIC.copyInto(frame)
    // Descriptor and window bytes stay zero; every zeroed block header is empty and non-final.
    return frame
}
