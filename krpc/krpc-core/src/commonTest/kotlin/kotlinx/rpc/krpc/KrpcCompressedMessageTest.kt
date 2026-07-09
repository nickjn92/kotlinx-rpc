/*
 * Copyright 2023-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.krpc

import kotlinx.coroutines.test.runTest
import kotlinx.rpc.krpc.internal.KrpcCompressionException
import kotlinx.rpc.krpc.internal.compressWith
import kotlinx.rpc.krpc.internal.decompressWith
import kotlin.io.encoding.Base64
import kotlin.js.JsName
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

class KrpcCompressedMessageTest {
    @Test
    @JsName("string_message_round_trips_through_a_string_envelope")
    fun `string message round trips through a string envelope`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)
        val original = KrpcTransportMessage.StringMessage(EXPANDED.repeat(64))

        val compressed = original.compressWith(compression, peerMaxDecompressedMessageSize = null)

        val envelope = assertIs<KrpcTransportMessage.StringMessage>(compressed)
        assertTrue(envelope.value.startsWith("krpc-cmp:"))
        assertTrue(envelope.value.length < original.value.length)

        val decompressed = assertIs<KrpcTransportMessage.StringMessage>(envelope.decompressWith(compression))
        assertEquals(original.value, decompressed.value)
        assertEquals(1, codec.compressions)
        assertEquals(1, codec.decompressions)
    }

    @Test
    @JsName("binary_message_round_trips_through_a_binary_envelope")
    fun `binary message round trips through a binary envelope`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)
        val original = KrpcTransportMessage.BinaryMessage(EXPANDED.repeat(64).encodeToByteArray())

        val compressed = original.compressWith(compression, peerMaxDecompressedMessageSize = null)

        val envelope = assertIs<KrpcTransportMessage.BinaryMessage>(compressed)
        assertContentEquals(MAGIC, envelope.value.copyOfRange(0, MAGIC.size))
        assertTrue(envelope.value.size < original.value.size)

        val decompressed = assertIs<KrpcTransportMessage.BinaryMessage>(envelope.decompressWith(compression))
        assertContentEquals(original.value, decompressed.value)
    }

    @Test
    @JsName("message_below_the_minimum_size_is_not_compressed")
    fun `message below the minimum size is not compressed`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 1024)
        val original = KrpcTransportMessage.StringMessage(EXPANDED)

        val result = original.compressWith(compression, peerMaxDecompressedMessageSize = null)

        assertSame(original, result)
        assertEquals(0, codec.compressions)
    }

    @Test
    @JsName("message_larger_than_the_peer_limit_is_not_compressed")
    fun `message larger than the peer limit is not compressed`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)
        val payload = EXPANDED.repeat(64)
        val original = KrpcTransportMessage.StringMessage(payload)

        val overLimit = original.compressWith(compression, peerMaxDecompressedMessageSize = payload.length - 1)
        assertSame(original, overLimit)
        assertEquals(0, codec.compressions)

        val atLimit = original.compressWith(compression, peerMaxDecompressedMessageSize = payload.length)
        val envelope = assertIs<KrpcTransportMessage.StringMessage>(atLimit)
        assertTrue(envelope.value.startsWith("krpc-cmp:"))
    }

    @Test
    @JsName("incompressible_message_is_sent_as_is")
    fun `incompressible message is sent as is`() = runTest {
        val compression = KrpcMessageCompression(IdentityCodec(), minimumMessageSize = 0)

        val binary = KrpcTransportMessage.BinaryMessage(EXPANDED.encodeToByteArray())
        assertSame(binary, binary.compressWith(compression, peerMaxDecompressedMessageSize = null))

        val string = KrpcTransportMessage.StringMessage(EXPANDED)
        assertSame(string, string.compressWith(compression, peerMaxDecompressedMessageSize = null))
    }

    @Test
    @JsName("string_message_with_an_unpaired_surrogate_is_rejected_before_compression")
    fun `string message with an unpaired surrogate is rejected before compression`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)
        val original = KrpcTransportMessage.StringMessage("\uD800" + EXPANDED.repeat(16))

        assertFailsWith<CharacterCodingException> {
            original.compressWith(compression, peerMaxDecompressedMessageSize = null)
        }
        assertEquals(0, codec.compressions)
    }

    @Test
    @JsName("messages_without_envelopes_pass_through_decompression")
    fun `messages without envelopes pass through decompression`() = runTest {
        val compression = KrpcMessageCompression(TokenCodec(), minimumMessageSize = 0)

        val string = KrpcTransportMessage.StringMessage("plain")
        assertSame(string, string.decompressWith(compression))

        val binary = KrpcTransportMessage.BinaryMessage(byteArrayOf(1, 2, 3))
        assertSame(binary, binary.decompressWith(compression))
    }

    @Test
    @JsName("string_envelope_is_not_parsed_without_negotiated_compression")
    fun `string envelope is not parsed without negotiated compression`() = runTest {
        val message = KrpcTransportMessage.StringMessage("krpc-cmp:not-negotiated")

        assertSame(message, message.decompressWith(compression = null))
    }

    @Test
    @JsName("magic_prefixed_messages_are_enveloped_even_when_passthrough_would_apply")
    fun `magic prefixed messages are enveloped even when passthrough would apply`() = runTest {
        // identity codec and a large minimum size: every passthrough path would normally apply
        val compression = KrpcMessageCompression(IdentityCodec(), minimumMessageSize = 1024)

        val binary = KrpcTransportMessage.BinaryMessage(MAGIC + byteArrayOf(1, 2, 3))
        val binaryEnvelope = assertIs<KrpcTransportMessage.BinaryMessage>(
            binary.compressWith(compression, peerMaxDecompressedMessageSize = null),
        )
        assertTrue(binaryEnvelope.value.size > binary.value.size)
        val binaryRoundTrip = assertIs<KrpcTransportMessage.BinaryMessage>(binaryEnvelope.decompressWith(compression))
        assertContentEquals(binary.value, binaryRoundTrip.value)

        val string = KrpcTransportMessage.StringMessage("krpc-cmp:looks-like-an-envelope")
        val stringEnvelope = assertIs<KrpcTransportMessage.StringMessage>(
            string.compressWith(compression, peerMaxDecompressedMessageSize = null),
        )
        assertTrue(stringEnvelope.value.length > string.value.length)
        val stringRoundTrip = assertIs<KrpcTransportMessage.StringMessage>(stringEnvelope.decompressWith(compression))
        assertEquals(string.value, stringRoundTrip.value)
    }

    @Test
    @JsName("magic_prefixed_message_over_the_peer_limit_fails_the_send")
    fun `magic prefixed message over the peer limit fails the send`() = runTest {
        val compression = KrpcMessageCompression(IdentityCodec(), minimumMessageSize = 0)
        val binary = KrpcTransportMessage.BinaryMessage(MAGIC + ByteArray(16))

        assertFailsWith<KrpcCompressionException> {
            binary.compressWith(compression, peerMaxDecompressedMessageSize = binary.value.size - 1)
        }
    }

    @Test
    @JsName("malformed_envelopes_fail_with_a_compression_exception")
    fun `malformed envelopes fail with a compression exception`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)

        // truncated header
        assertFailsWith<KrpcCompressionException> {
            KrpcTransportMessage.BinaryMessage(MAGIC).decompressWith(compression)
        }

        // unsupported version
        assertFailsWith<KrpcCompressionException> {
            KrpcTransportMessage.BinaryMessage(binaryEnvelope(9, 1, codec.name, ByteArray(4)))
                .decompressWith(compression)
        }

        // codec name mismatch
        assertFailsWith<KrpcCompressionException> {
            KrpcTransportMessage.BinaryMessage(binaryEnvelope(1, 1, "other", ByteArray(4)))
                .decompressWith(compression)
        }

        // invalid UTF-8 codec name
        val invalidCodecName = MAGIC + byteArrayOf(1, 1, 1, 0xff.toByte()) + ByteArray(4)
        val invalidCodecNameException = assertFailsWith<KrpcCompressionException> {
            KrpcTransportMessage.BinaryMessage(invalidCodecName).decompressWith(compression)
        }
        assertContains(invalidCodecNameException.message.orEmpty(), "valid UTF-8")

        // unsupported message type
        assertFailsWith<KrpcCompressionException> {
            KrpcTransportMessage.BinaryMessage(binaryEnvelope(1, 9, codec.name, "#".encodeToByteArray()))
                .decompressWith(compression)
        }

        // invalid Base64 in a string envelope
        assertFailsWith<KrpcCompressionException> {
            KrpcTransportMessage.StringMessage("krpc-cmp:???").decompressWith(compression)
        }

        // valid Base64 that is not an envelope
        assertFailsWith<KrpcCompressionException> {
            KrpcTransportMessage.StringMessage("krpc-cmp:" + Base64.encode("garbage".encodeToByteArray()))
                .decompressWith(compression)
        }
    }

    @Test
    @JsName("codec_decompression_failure_is_wrapped_into_a_compression_exception")
    fun `codec decompression failure is wrapped into a compression exception`() = runTest {
        val codec = object : KrpcMessageCodec {
            override val name: String = "failing"

            override suspend fun compress(data: ByteArray): ByteArray = data

            override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray =
                error("corrupted")
        }
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)
        val envelope = KrpcTransportMessage.BinaryMessage(binaryEnvelope(1, 2, codec.name, ByteArray(4)))

        val exception = assertFailsWith<KrpcCompressionException> {
            envelope.decompressWith(compression)
        }

        assertIs<IllegalStateException>(exception.cause)
    }

    @Test
    @JsName("compressed_payload_over_the_limit_is_rejected_before_decompression")
    fun `compressed payload over the limit is rejected before decompression`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, maxDecompressedMessageSize = 4)
        val envelope = KrpcTransportMessage.BinaryMessage(binaryEnvelope(1, 2, codec.name, ByteArray(5)))

        assertFailsWith<KrpcCompressionException> {
            envelope.decompressWith(compression)
        }
        assertEquals(0, codec.decompressions)
    }

    @Test
    @JsName("oversized_string_envelope_is_rejected_before_base64_decoding")
    fun `oversized string envelope is rejected before Base64 decoding`() = runTest {
        val codec = TokenCodec()
        val compression = KrpcMessageCompression(codec, maxDecompressedMessageSize = 4)
        val envelope = KrpcTransportMessage.StringMessage("krpc-cmp:" + "!".repeat(364))

        val exception = assertFailsWith<KrpcCompressionException> {
            envelope.decompressWith(compression)
        }

        assertContains(exception.message.orEmpty(), "size limit")
        assertEquals(0, codec.decompressions)
    }

    @Test
    @JsName("oversized_codec_output_is_rejected")
    fun `oversized codec output is rejected`() = runTest {
        var decompressions = 0
        val codec = object : KrpcMessageCodec {
            override val name: String = "expanding"

            override suspend fun compress(data: ByteArray): ByteArray = data

            override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray {
                decompressions++
                return ByteArray(5)
            }
        }
        val compression = KrpcMessageCompression(codec, maxDecompressedMessageSize = 4)
        val envelope = KrpcTransportMessage.BinaryMessage(binaryEnvelope(1, 2, codec.name, byteArrayOf(1)))

        val exception = assertFailsWith<KrpcCompressionException> {
            envelope.decompressWith(compression)
        }

        assertContains(exception.message.orEmpty(), "configured limit")
        assertEquals(1, decompressions)
    }

    @Test
    @JsName("codec_name_must_be_valid_utf8")
    fun `codec name must be valid UTF-8`() {
        val codec = object : KrpcMessageCodec {
            override val name: String = "\uD800"

            override suspend fun compress(data: ByteArray): ByteArray = data

            override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray = data
        }

        val exception = assertFailsWith<IllegalArgumentException> {
            KrpcMessageCompression(codec)
        }

        assertContains(exception.message.orEmpty(), "valid UTF-8")
    }
}

private val EXPANDED = "a".repeat(64)
private val MAGIC = "krpc-cmp".encodeToByteArray()

private fun binaryEnvelope(version: Byte, messageType: Byte, codecName: String, payload: ByteArray): ByteArray {
    val name = codecName.encodeToByteArray()

    return MAGIC + byteArrayOf(version, messageType, name.size.toByte()) + name + payload
}

private class TokenCodec : KrpcMessageCodec {
    var compressions = 0
        private set

    var decompressions = 0
        private set

    override val name: String = "token"

    override suspend fun compress(data: ByteArray): ByteArray {
        compressions++

        return data.decodeToString().replace(EXPANDED, "#").encodeToByteArray()
    }

    override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray {
        decompressions++

        return data.decodeToString().replace("#", EXPANDED).encodeToByteArray()
    }
}

private class IdentityCodec : KrpcMessageCodec {
    override val name: String = "identity"

    override suspend fun compress(data: ByteArray): ByteArray = data

    override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray = data
}
