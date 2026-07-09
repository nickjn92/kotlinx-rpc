/*
 * Copyright 2023-2025 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

@file:OptIn(ExperimentalCoroutinesApi::class, ExperimentalSerializationApi::class)

package kotlinx.rpc.krpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.withTimeout
import kotlinx.rpc.krpc.internal.HandlerKey
import kotlinx.rpc.krpc.internal.KrpcCallMessage
import kotlinx.rpc.krpc.internal.KrpcConnector
import kotlinx.rpc.krpc.internal.KrpcGenericMessage
import kotlinx.rpc.krpc.internal.KrpcMessage
import kotlinx.rpc.krpc.internal.KrpcPlugin
import kotlinx.rpc.krpc.internal.KrpcPluginKey
import kotlinx.rpc.krpc.internal.KrpcProtocolMessage
import kotlinx.rpc.krpc.internal.decompressWith
import kotlinx.rpc.krpc.internal.deserialize
import kotlinx.rpc.test.runTestWithCoroutinesProbes
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialFormat
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.StringFormat
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlin.coroutines.CoroutineContext
import kotlin.js.JsName
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class KrpcConnectorTest : KrpcConnectorBaseTest() {
    @Test
    @JsName("service_call_success_is_delivered_to_a_client")
    fun `service call success is delivered to a client`() = runTest { client, server ->
        // Enable backpressure to use configured perCallBufferSize deterministically
        val clientPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val serverPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val hs = handshakeBoth(client, server, clientPExceptionsChannel, serverPExceptionsChannel)

        val clientInbox = Channel<KrpcMessage>(10)

        client.subscribeToMessages(HandlerKey.ServiceCall("svc", "1")) {
            clientInbox.send(it)
        }

        server.subscribeToMessages(HandlerKey.Service("svc")) { message ->
            if (message is KrpcCallMessage.CallDataString) {
                server.sendMessage(
                    KrpcCallMessage.CallSuccessString(
                        callId = message.callId,
                        serviceType = message.serviceType,
                        data = "${message.data} : OK",
                        connectionId = message.connectionId,
                        serviceId = message.serviceId,
                    )
                )
            }
        }

        client.sendMessage("ping".asCallMessage("svc", "1"))

        val reply = clientInbox.receive()
        val success = assertIs<KrpcCallMessage.CallSuccessString>(reply)
        assertEquals("ping : OK", success.data)

        hs.assertAllShookHands()

        assertTrue(clientPExceptionsChannel.isEmpty)
        assertTrue(serverPExceptionsChannel.isEmpty)
    }

    @Test
    @JsName("server_exception_propagates_as_a_call_exception")
    fun `server exception propagates as a call exception`() = runTest { client, server ->
        val clientPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val serverPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val hs = handshakeBoth(client, server, clientPExceptionsChannel, serverPExceptionsChannel)

        val clientInbox = Channel<KrpcMessage>(10)
        client.subscribeToMessages(HandlerKey.ServiceCall("svc", "2")) {
            clientInbox.send(it)
        }

        server.subscribeToMessages(HandlerKey.Service("svc")) {
            throw IllegalArgumentException("boom")
        }

        server.subscribeToMessages(HandlerKey.Generic) {
            throw IllegalArgumentException("boom")
        }

        client.sendMessage("req".asCallMessage("svc", "2"))

        val msg = clientInbox.receive()
        val ex = assertIs<KrpcCallMessage.CallException>(msg)
        val t = ex.cause.deserialize()
        assertTrue(t.message!!.contains("Failed to process call"), t.toString())

        client.sendMessage("req".asGenericMessage())
        val protocolMessage = clientPExceptionsChannel.receive()
        val protocolException = assertIs<KrpcProtocolMessage.Failure>(protocolMessage)
        assertEquals("req".asGenericMessage(), protocolException.failedMessage)

        hs.assertAllShookHands()
    }

    @Test
    @JsName("server_timeout_sends_a_call_exception")
    fun `server timeout sends a call exception`() = runTest(callTimeout = 200.milliseconds) { client, server ->
        val clientPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val serverPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val hs = handshakeBoth(client, server, clientPExceptionsChannel, serverPExceptionsChannel)

        val clientInbox = Channel<KrpcMessage>(10)
        client.subscribeToMessages(HandlerKey.ServiceCall("svc", "3")) {
            clientInbox.send(it)
        }

        server.subscribeToMessages(HandlerKey.Service("svc")) {
            // Simulate long processing exceeding callTimeout
            delay(500.milliseconds)
        }

        client.sendMessage("req".asCallMessage("svc", "3"))

        val msg = clientInbox.receive()

        val ex = assertIs<KrpcCallMessage.CallException>(msg)
        val t = ex.cause.deserialize()
        assertTrue(t.message!!.contains("Failed to process call"), t.toString())
        assertEquals(t.cause?.message?.contains("Timeout while processing message"), true, t.toString())

        hs.assertAllShookHands()
        assertTrue(clientPExceptionsChannel.isEmpty)
        assertTrue(serverPExceptionsChannel.isEmpty)
    }

    @Test
    @JsName("buffer_overflow_does_not_result_in_a_call_exception")
    fun `buffer overflow does not result in a call exception`() = runTest(perCallBufferSize = 1) { client, server ->
        val clientPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val serverPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val hs = handshakeBoth(client, server, clientPExceptionsChannel, serverPExceptionsChannel, perCallBufferSize = 1)

        val clientInbox = Channel<KrpcMessage>(10)
        client.subscribeToMessages(HandlerKey.ServiceCall("svc", "4")) {
            if (it is KrpcCallMessage.CallException) {
                val ex = it.cause.deserialize()

                coroutineContext.cancel(CancellationException("Unexpected failure from the server", ex))
                return@subscribeToMessages
            }

            clientInbox.send(it)
        }

        val serverInbox = Channel<KrpcMessage>(10)
        server.subscribeToMessages(HandlerKey.Service("svc")) {
            if (it is KrpcCallMessage.CallException) {
                val ex = it.cause.deserialize()

                coroutineContext.cancel(CancellationException("Unexpected failure from the client", ex))
                return@subscribeToMessages
            }

            serverInbox.send(it)
        }

        client.sendMessage("m1".asCallMessage("svc", "4"))
        client.sendMessage("m2".asCallMessage("svc", "4"))

        val msg = serverInbox.receive()
        assertEquals("m1", (msg as KrpcCallMessage.CallDataString).data)
        val msg2 = serverInbox.receive()
        assertEquals("m2", (msg2 as KrpcCallMessage.CallDataString).data)

        hs.assertAllShookHands()
        assertTrue(clientInbox.isEmpty)
        assertTrue(clientPExceptionsChannel.isEmpty)
        assertTrue(serverPExceptionsChannel.isEmpty)
    }

    @Test
    @JsName("wait_timeout_exceeds_sends_a_call_exception")
    fun `wait timeout exceeds sends a call exception`() = runTest(
        waitTimeout = 300.milliseconds,
        perCallBufferSize = 2,
    ) { client, server ->
        val clientPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val serverPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val hs = handshakeBoth(client, server, clientPExceptionsChannel, serverPExceptionsChannel, perCallBufferSize = 2)

        val clientInbox = Channel<KrpcMessage>(10)
        client.subscribeToMessages(HandlerKey.ServiceCall("svc", "5")) {
            clientInbox.send(it)
        }

        // Send one message that will stay unprocessed until waitTimeout triggers discard
        client.sendMessage("stay".asCallMessage("svc", "5"))
        client.sendMessage("stay2".asCallMessage("svc", "5"))

        val msg = clientInbox.receive()
        val ex = assertIs<KrpcCallMessage.CallException>(msg)
        val top = ex.cause.deserialize()
        // top-level message comes from buffer close; nested cause contains a wait-timeout message
        assertEquals(top.message?.contains("2 messages were unprocessed"), true, top.toString())
        assertEquals(top.cause?.message?.contains("Waiting limit of"), true, top.toString())

        hs.assertAllShookHands()
    }

    @Test
    @JsName("one_item_buffer_size_works_with_a_stream_of_messages")
    fun `one item buffer size works with a stream of messages`() = runTest(perCallBufferSize = 1) { client, server ->
        val clientPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val serverPExceptionsChannel = Channel<KrpcProtocolMessage.Failure>(Channel.UNLIMITED)
        val hs = handshakeBoth(client, server, clientPExceptionsChannel, serverPExceptionsChannel, perCallBufferSize = 1)

        val clientInbox = Channel<KrpcMessage>(10)
        client.subscribeToMessages(HandlerKey.ServiceCall("svc", "6")) {
            clientInbox.send(it)
        }

        server.subscribeToMessages(HandlerKey.Service("svc")) { message ->
            server.sendMessage(message)
        }

        launch {
            repeat(100) {
                client.sendMessage("ping".asCallMessage("svc", "6"))
            }
        }

        repeat(100) {
            assertEquals("ping", (clientInbox.receive() as KrpcCallMessage.CallDataString).data)
        }

        hs.assertAllShookHands()
    }

    @Test
    @JsName("negotiated_message_compression_round_trips_service_messages")
    fun `negotiated message compression round trips service messages`() = run {
        val codec = ReplacingCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)

        runTest(messageCompression = compression) { client, server ->
            val hs = handshakeBothWithCompression(client, server, codec.name)

            val serverInbox = Channel<KrpcMessage>(10)
            server.subscribeToMessages(HandlerKey.Service("svc")) {
                serverInbox.send(it)
            }

            // large enough that compression wins even with the Base64 overhead of the string envelope
            val data = ReplacingCodec.EXPANDED.repeat(8)
            client.sendMessage(data.asCallMessage("svc", "7"))

            val message = assertIs<KrpcCallMessage.CallDataString>(serverInbox.receive())
            assertEquals(data, message.data)
            assertTrue(codec.compressions > 0)
            assertTrue(codec.decompressions > 0)

            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("negotiated_message_compression_round_trips_binary_service_messages")
    fun `negotiated message compression round trips binary service messages`() = run {
        val codec = ReplacingCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)

        runTest(serialFormat = JsonBinaryFormat, messageCompression = compression) { client, server ->
            val hs = handshakeBothWithCompression(client, server, codec.name)

            val serverInbox = Channel<KrpcMessage>(10)
            server.subscribeToMessages(HandlerKey.Service("svc")) {
                serverInbox.send(it)
            }

            client.sendMessage(ReplacingCodec.EXPANDED.asCallMessage("svc", "8"))

            val message = assertIs<KrpcCallMessage.CallDataString>(serverInbox.receive())
            assertEquals(ReplacingCodec.EXPANDED, message.data)
            assertTrue(codec.compressions > 0)
            assertTrue(codec.decompressions > 0)

            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("message_compression_is_not_used_when_only_one_peer_configures_it")
    fun `message compression is not used when only one peer configures it`() = run {
        val codec = ReplacingCodec()

        runTest(clientMessageCompression = KrpcMessageCompression(codec, minimumMessageSize = 0)) { client, server ->
            val hs = handshakeBothWithCompression(client, server, clientCodecName = codec.name, serverCodecName = null)

            val serverInbox = Channel<KrpcMessage>(10)
            server.subscribeToMessages(HandlerKey.Service("svc")) {
                serverInbox.send(it)
            }

            client.sendMessage(ReplacingCodec.EXPANDED.asCallMessage("svc", "9"))

            val message = assertIs<KrpcCallMessage.CallDataString>(serverInbox.receive())
            assertEquals(ReplacingCodec.EXPANDED, message.data)
            assertEquals(0, codec.compressions)
            assertEquals(0, codec.decompressions)

            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("message_compression_is_not_used_when_codec_names_differ")
    fun `message compression is not used when codec names differ`() = run {
        val clientCodec = ReplacingCodec(name = "client-replacing")
        val serverCodec = ReplacingCodec(name = "server-replacing")

        runTest(
            clientMessageCompression = KrpcMessageCompression(clientCodec, minimumMessageSize = 0),
            serverMessageCompression = KrpcMessageCompression(serverCodec, minimumMessageSize = 0),
        ) { client, server ->
            val hs = handshakeBothWithCompression(
                client = client,
                server = server,
                clientCodecName = clientCodec.name,
                serverCodecName = serverCodec.name,
            )

            val serverInbox = Channel<KrpcMessage>(10)
            server.subscribeToMessages(HandlerKey.Service("svc")) {
                serverInbox.send(it)
            }

            client.sendMessage(ReplacingCodec.EXPANDED.asCallMessage("svc", "10"))

            val message = assertIs<KrpcCallMessage.CallDataString>(serverInbox.receive())
            assertEquals(ReplacingCodec.EXPANDED, message.data)
            assertEquals(0, clientCodec.compressions)
            assertEquals(0, clientCodec.decompressions)
            assertEquals(0, serverCodec.compressions)
            assertEquals(0, serverCodec.decompressions)

            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("messages_larger_than_the_peer_max_decompressed_size_are_sent_uncompressed")
    fun `messages larger than the peer max decompressed size are sent uncompressed`() = run {
        val clientCodec = ReplacingCodec()
        val serverCodec = ReplacingCodec()
        val serverMax = 32

        runTest(
            clientMessageCompression = KrpcMessageCompression(clientCodec, minimumMessageSize = 0),
            serverMessageCompression = KrpcMessageCompression(
                codec = serverCodec,
                minimumMessageSize = 0,
                maxDecompressedMessageSize = serverMax,
            ),
        ) { client, server ->
            val hs = handshakeBothWithCompression(
                client = client,
                server = server,
                clientCodecName = clientCodec.name,
                serverMaxDecompressedMessageSize = serverMax,
            )

            val serverInbox = Channel<KrpcMessage>(10)
            server.subscribeToMessages(HandlerKey.Service("svc")) {
                serverInbox.send(it)
            }

            client.sendMessage(ReplacingCodec.EXPANDED.asCallMessage("svc", "11"))

            val message = assertIs<KrpcCallMessage.CallDataString>(serverInbox.receive())
            assertEquals(ReplacingCodec.EXPANDED, message.data)
            assertEquals(0, clientCodec.compressions)
            assertEquals(0, serverCodec.decompressions)

            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("compression_failure_falls_back_to_uncompressed_delivery")
    fun `compression failure falls back to uncompressed delivery`() = run {
        val codec = object : KrpcMessageCodec {
            override val name: String = "throwing"

            override suspend fun compress(data: ByteArray): ByteArray = error("compress failed")

            override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray =
                error("decompress failed")
        }

        runTest(messageCompression = KrpcMessageCompression(codec, minimumMessageSize = 0)) { client, server ->
            val hs = handshakeBothWithCompression(client, server, clientCodecName = codec.name)

            val serverInbox = Channel<KrpcMessage>(10)
            server.subscribeToMessages(HandlerKey.Service("svc")) {
                serverInbox.send(it)
            }

            client.sendMessage(ReplacingCodec.EXPANDED.asCallMessage("svc", "12"))

            val message = assertIs<KrpcCallMessage.CallDataString>(serverInbox.receive())
            assertEquals(ReplacingCodec.EXPANDED, message.data)

            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("undecodable_compressed_message_cancels_the_connection")
    fun `undecodable compressed message cancels the connection`() = run {
        val clientCodec = ReplacingCodec()
        val decompressionAttempted = CompletableDeferred<Unit>()
        val failingCodec = object : KrpcMessageCodec {
            override val name: String = clientCodec.name

            override suspend fun compress(data: ByteArray): ByteArray = data

            override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray {
                decompressionAttempted.complete(Unit)
                error("corrupted")
            }
        }

        runTest(
            clientMessageCompression = KrpcMessageCompression(clientCodec, minimumMessageSize = 256),
            serverMessageCompression = KrpcMessageCompression(failingCodec, minimumMessageSize = 256),
        ) { client, server ->
            val hs = handshakeBothWithCompression(client, server, clientCodecName = clientCodec.name)

            client.sendMessage(ReplacingCodec.EXPANDED.repeat(8).asCallMessage("svc", "13"))

            decompressionAttempted.await()
            withTimeout(1.seconds) {
                server.transportScope.coroutineContext.job.join()
            }
            assertTrue(server.transportScope.coroutineContext.job.isCancelled)

            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("protocol_messages_with_the_compression_prefix_are_enveloped")
    fun `protocol messages with the compression prefix are enveloped`() = run {
        val codec = ReplacingCodec()
        val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)

        runTest(serialFormat = PrefixJsonFormat, messageCompression = compression) { client, server ->
            val hs = handshakeBothWithCompression(client, server, clientCodecName = codec.name)
            val serverInbox = Channel<KrpcMessage>(1)
            server.unsubscribeFromMessages(HandlerKey.Protocol)
            server.subscribeToMessages(HandlerKey.Protocol) {
                if (it is KrpcProtocolMessage.Failure) serverInbox.send(it)
            }

            client.sendMessage(KrpcProtocolMessage.Failure(errorMessage = "expected"))

            val message = withTimeout(1.seconds) {
                assertIs<KrpcProtocolMessage.Failure>(serverInbox.receive())
            }
            assertEquals("expected", message.errorMessage)
            assertTrue(codec.compressions > 0)
            assertTrue(codec.decompressions > 0)
            hs.assertAllShookHands()
        }
    }

    @Test
    @JsName("binary_message_with_compression_magic_is_unchanged_without_negotiated_compression")
    fun `binary message with compression magic is unchanged without negotiated compression`() =
        runTestWithCoroutinesProbes(timeout = 15.seconds) {
            val bytes = "krpc-cmp-not-negotiated".encodeToByteArray()

            val result = KrpcTransportMessage.BinaryMessage(bytes).decompressWith(compression = null)

            val binaryMessage = assertIs<KrpcTransportMessage.BinaryMessage>(result)
            assertContentEquals(bytes, binaryMessage.value)
        }

    private class HsResult {
        val clientShookHands = CompletableDeferred<Unit>()
        val serverShookHands = CompletableDeferred<Unit>()

        fun assertAllShookHands() {
            assertTrue(clientShookHands.isCompleted)
            assertTrue(serverShookHands.isCompleted)
        }

        suspend fun await() {
            clientShookHands.await()
            serverShookHands.await()
        }
    }

    private suspend fun handshakeBoth(
        client: KrpcConnector,
        server: KrpcConnector,
        clientExceptionsChannel: Channel<KrpcProtocolMessage.Failure>,
        serverExceptionsChannel: Channel<KrpcProtocolMessage.Failure>,
        perCallBufferSize: Int = 100,
    ): HsResult {
        val hs = KrpcProtocolMessage.Handshake(KrpcPlugin.ALL)
        val expectedHs = expectedHandshake(perCallBufferSize)
        val hsResult = HsResult()

        client.subscribeToMessages(HandlerKey.Protocol) {
            if (it is KrpcProtocolMessage.Failure) {
                if (!hsResult.clientShookHands.isCompleted) {
                    fail("Handshake must be first message, but got: $it")
                }

                clientExceptionsChannel.send(it)
            }
            assertEquals(expectedHs, it)
            hsResult.clientShookHands.complete(Unit)
        }

        server.subscribeToMessages(HandlerKey.Protocol) {
            if (it is KrpcProtocolMessage.Failure) {
                if (!hsResult.serverShookHands.isCompleted) {
                    fail("Handshake must be first message, but got: $it")
                }

                serverExceptionsChannel.send(it)
            }
            assertEquals(expectedHs, it)
            hsResult.serverShookHands.complete(Unit)
        }

        client.sendMessage(hs)
        server.sendMessage(hs)

        hsResult.await()

        return hsResult
    }

    private suspend fun handshakeBothWithCompression(
        client: KrpcConnector,
        server: KrpcConnector,
        clientCodecName: String,
        serverCodecName: String? = clientCodecName,
        clientMaxDecompressedMessageSize: Int = KrpcMessageCompression.DEFAULT_MAX_DECOMPRESSED_MESSAGE_SIZE,
        serverMaxDecompressedMessageSize: Int = KrpcMessageCompression.DEFAULT_MAX_DECOMPRESSED_MESSAGE_SIZE,
    ): HsResult {
        val hs = KrpcProtocolMessage.Handshake(KrpcPlugin.ALL)
        val clientExpectedHs = expectedHandshake(
            codecName = serverCodecName,
            maxDecompressedMessageSize = serverMaxDecompressedMessageSize,
        )
        val serverExpectedHs = expectedHandshake(
            codecName = clientCodecName,
            maxDecompressedMessageSize = clientMaxDecompressedMessageSize,
        )
        val hsResult = HsResult()

        client.subscribeToMessages(HandlerKey.Protocol) {
            assertEquals(clientExpectedHs, it)
            hsResult.clientShookHands.complete(Unit)
        }

        server.subscribeToMessages(HandlerKey.Protocol) {
            assertEquals(serverExpectedHs, it)
            hsResult.serverShookHands.complete(Unit)
        }

        client.sendMessage(hs)
        server.sendMessage(hs)

        hsResult.await()

        return hsResult
    }

    private fun expectedHandshake(
        perCallBufferSize: Int = 100,
        codecName: String? = null,
        maxDecompressedMessageSize: Int = KrpcMessageCompression.DEFAULT_MAX_DECOMPRESSED_MESSAGE_SIZE,
    ): KrpcProtocolMessage.Handshake {
        val pluginParams = mutableMapOf(
            KrpcPluginKey.WINDOW_UPDATE to "$perCallBufferSize",
        )

        return if (codecName == null) {
            KrpcProtocolMessage.Handshake(KrpcPlugin.ALL, pluginParams = pluginParams)
        } else {
            pluginParams[KrpcPluginKey.MESSAGE_COMPRESSION] = codecName
            pluginParams[KrpcPluginKey.MESSAGE_COMPRESSION_MAX_SIZE] = "$maxDecompressedMessageSize"
            pluginParams[KrpcPluginKey.MESSAGE_COMPRESSION_ENVELOPE_VERSION] = "1"
            KrpcProtocolMessage.Handshake(
                supportedPlugins = KrpcPlugin.ALL + KrpcPlugin.MESSAGE_COMPRESSION,
                pluginParams = pluginParams,
            )
        }
    }

    private class ReplacingCodec(
        override val name: String = "replacing",
    ) : KrpcMessageCodec {
        var compressions = 0
            private set

        var decompressions = 0
            private set

        override suspend fun compress(data: ByteArray): ByteArray {
            compressions++

            return data.decodeToString().replace(EXPANDED, TOKEN).encodeToByteArray()
        }

        override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray {
            decompressions++

            return data.decodeToString().replace(TOKEN, EXPANDED).encodeToByteArray()
        }

        companion object {
            const val EXPANDED: String = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            const val TOKEN: String = "#"
        }
    }
}

abstract class KrpcConnectorBaseTest {
    protected fun String.asCallMessage(
        serviceId: String,
        callId: String,
    ) = KrpcCallMessage.CallDataString(
        connectionId = null,
        callId = callId,
        serviceType = serviceId,
        data = this,
        callableName = "",
        callType = KrpcCallMessage.CallType.Method,
    )

    protected fun String.asGenericMessage() = KrpcGenericMessage(
        connectionId = null,
        pluginParams = mapOf(KrpcPluginKey.GENERIC_MESSAGE_TYPE to this),
    )

    protected fun runTest(
        testTimeout: Duration = 15.seconds,
        waitTimeout: Duration = 1.seconds,
        callTimeout: Duration = 1.seconds,
        perCallBufferSize: Int = 100,
        serialFormat: SerialFormat = Json,
        messageCompression: KrpcMessageCompression? = null,
        clientMessageCompression: KrpcMessageCompression? = messageCompression,
        serverMessageCompression: KrpcMessageCompression? = messageCompression,
        body: suspend TestScope.(clientConnector: KrpcConnector, serverConnector: KrpcConnector) -> Unit,
    ) = runTestWithCoroutinesProbes(timeout = testTimeout) {
        val clientConnectorConfig = KrpcConfig.Connector(
            waitTimeout = waitTimeout,
            callTimeout = callTimeout,
            perCallBufferSize = perCallBufferSize,
            messageCompression = clientMessageCompression,
        )
        val serverConnectorConfig = KrpcConfig.Connector(
            waitTimeout = waitTimeout,
            callTimeout = callTimeout,
            perCallBufferSize = perCallBufferSize,
            messageCompression = serverMessageCompression,
        )

        val transport = LocalTransport(coroutineContext)

        val clientConnector = KrpcConnector(
            serialFormat = serialFormat,
            transport = transport.client,
            config = clientConnectorConfig,
            isServer = false,
        )

        val serverConnector = KrpcConnector(
            serialFormat = serialFormat,
            transport = transport.server,
            config = serverConnectorConfig,
            isServer = true,
        )

        try {
            body(clientConnector, serverConnector)
        } finally {
            transport.coroutineContext.job.cancelAndJoin()
        }
    }
}

private object JsonBinaryFormat : BinaryFormat {
    override val serializersModule: SerializersModule = Json.serializersModule

    override fun <T> encodeToByteArray(serializer: SerializationStrategy<T>, value: T): ByteArray {
        return Json.encodeToString(serializer, value).encodeToByteArray()
    }

    override fun <T> decodeFromByteArray(deserializer: DeserializationStrategy<T>, bytes: ByteArray): T {
        return Json.decodeFromString(deserializer, bytes.decodeToString())
    }
}

internal object PrefixJsonFormat : StringFormat {
    private const val PREFIX = "krpc-cmp:"

    override val serializersModule: SerializersModule = Json.serializersModule

    override fun <T> encodeToString(serializer: SerializationStrategy<T>, value: T): String {
        return PREFIX + Json.encodeToString(serializer, value)
    }

    override fun <T> decodeFromString(deserializer: DeserializationStrategy<T>, string: String): T {
        return Json.decodeFromString(deserializer, string.removePrefix(PREFIX))
    }
}

internal class LocalTransport(
    parentContext: CoroutineContext? = null,
) : CoroutineScope {
    override val coroutineContext = SupervisorJob(parentContext?.get(Job))

    private val clientIncoming = Channel<KrpcTransportMessage>()
    private val serverIncoming = Channel<KrpcTransportMessage>()

    val client: KrpcTransport = object : KrpcTransport {
        override val coroutineContext: CoroutineContext = Job(this@LocalTransport.coroutineContext.job).let {
            if (parentContext != null) parentContext + it else it
        }

        override suspend fun send(message: KrpcTransportMessage) {
            serverIncoming.send(message)
        }

        override suspend fun receive(): KrpcTransportMessage {
            return clientIncoming.receive()
        }
    }

    val server: KrpcTransport = object : KrpcTransport {
        override val coroutineContext: CoroutineContext = Job(this@LocalTransport.coroutineContext.job).let {
            if (parentContext != null) parentContext + it else it
        }

        override suspend fun send(message: KrpcTransportMessage) {
            clientIncoming.send(message)
        }

        override suspend fun receive(): KrpcTransportMessage {
            return serverIncoming.receive()
        }
    }
}
