/*
 * Copyright 2023-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.krpc

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeout
import kotlinx.rpc.krpc.internal.HandlerKey
import kotlinx.rpc.krpc.internal.KrpcConnector
import kotlinx.rpc.krpc.internal.KrpcPlugin
import kotlinx.rpc.krpc.internal.KrpcPluginKey
import kotlinx.rpc.krpc.internal.KrpcProtocolMessage
import kotlinx.rpc.test.runTestWithCoroutinesProbes
import kotlinx.serialization.json.Json
import kotlin.js.JsName
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class KrpcCompressionNegotiationTest : KrpcConnectorBaseTest() {
    @Test
    @JsName("a_blocked_compressed_decode_does_not_block_another_connection")
    fun `a blocked compressed decode does not block another connection`() =
        runTestWithCoroutinesProbes(timeout = 15.seconds) {
            val started = Channel<Int>(Channel.UNLIMITED)
            val releases = List(2) { CompletableDeferred<Unit>() }
            val connections = List(2) { index ->
                val delegate = TestCompressionCodec()
                val codec = object : KrpcMessageCodec {
                    override val name: String = delegate.name

                    override suspend fun compress(data: ByteArray): ByteArray = delegate.compress(data)

                    override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray {
                        started.send(index)
                        releases[index].await()
                        return delegate.decompress(data, maxDecompressedSize)
                    }
                }
                val compression = KrpcMessageCompression(codec, minimumMessageSize = 0)
                val config = KrpcConfig.Connector(
                    waitTimeout = 1.seconds,
                    callTimeout = 1.seconds,
                    perCallBufferSize = 100,
                    messageCompression = compression,
                )
                val transport = LocalTransport(coroutineContext)
                Triple(
                    transport,
                    KrpcConnector(Json, transport.client, config, isServer = false),
                    KrpcConnector(Json, transport.server, config, isServer = true),
                )
            }

            try {
                connections.forEach { (_, client, server) ->
                    handshakeBothWithCompression(client, server)
                    server.subscribeToMessages(HandlerKey.Service("svc")) { }
                }

                connections.forEachIndexed { index, (_, client, _) ->
                    client.sendMessage(TestCompressionCodec.EXPANDED.repeat(8).asCallMessage("svc", "decode-$index"))
                }

                val first = withTimeout(1.seconds) { started.receive() }
                val second = withTimeout(1.seconds) { started.receive() }
                assertNotEquals(first, second)
            } finally {
                releases.forEach { it.complete(Unit) }
                connections.forEach { (transport, _, _) ->
                    transport.coroutineContext.job.cancelAndJoin()
                }
            }
        }

    @Test
    @JsName("malformed_present_compression_handshake_fields_disable_compression")
    fun `malformed present compression handshake fields disable compression`() = run {
        val codec = TestCompressionCodec()
        val invalidFields = listOf(
            KrpcPluginKey.MESSAGE_COMPRESSION_ENVELOPE_VERSION to "invalid",
            KrpcPluginKey.MESSAGE_COMPRESSION_MAX_SIZE to "invalid",
            KrpcPluginKey.MESSAGE_COMPRESSION_MAX_SIZE to "0",
            KrpcPluginKey.MESSAGE_COMPRESSION_MAX_SIZE to "-1",
        )
        runTest(
            serialFormat = PrefixJsonFormat,
            clientMessageCompression = KrpcMessageCompression(codec, minimumMessageSize = 0),
        ) { client, server ->
            val handshakes = Channel<Unit>(Channel.UNLIMITED)
            client.subscribeToMessages(HandlerKey.Protocol) {
                if (it is KrpcProtocolMessage.Handshake) handshakes.send(Unit)
            }

            invalidFields.forEachIndexed { index, (invalidKey, invalidValue) ->
                sendPeerHandshake(server, codec.name) {
                    this[invalidKey] = invalidValue
                }
                handshakes.receive()

                client.sendMessage(TestCompressionCodec.EXPANDED.asCallMessage("svc", "invalid-$index"))
                assertEquals(0, codec.compressions)
            }
        }
    }

    @Test
    @JsName("missing_compression_handshake_fields_remain_legacy_compatible")
    fun `missing compression handshake fields remain legacy compatible`() = run {
        val codec = TestCompressionCodec()
        runTest(
            serialFormat = PrefixJsonFormat,
            clientMessageCompression = KrpcMessageCompression(codec, minimumMessageSize = 0),
        ) { client, server ->
            val handshakeReceived = CompletableDeferred<Unit>()
            client.subscribeToMessages(HandlerKey.Protocol) {
                if (it is KrpcProtocolMessage.Handshake) handshakeReceived.complete(Unit)
            }
            sendPeerHandshake(server, codec.name, includeOptionalFields = false)
            handshakeReceived.await()

            client.sendMessage(TestCompressionCodec.EXPANDED.asCallMessage("svc", "legacy"))
            assertTrue(codec.compressions > 0)
        }
    }

    private suspend fun handshakeBothWithCompression(client: KrpcConnector, server: KrpcConnector): Unit {
        val clientReceived = CompletableDeferred<Unit>()
        val serverReceived = CompletableDeferred<Unit>()
        client.subscribeToMessages(HandlerKey.Protocol) {
            if (it is KrpcProtocolMessage.Handshake) clientReceived.complete(Unit)
        }
        server.subscribeToMessages(HandlerKey.Protocol) {
            if (it is KrpcProtocolMessage.Handshake) serverReceived.complete(Unit)
        }

        val handshake = KrpcProtocolMessage.Handshake(KrpcPlugin.ALL)
        client.sendMessage(handshake)
        server.sendMessage(handshake)
        clientReceived.await()
        serverReceived.await()
    }

    private suspend fun sendPeerHandshake(
        server: KrpcConnector,
        codecName: String,
        includeOptionalFields: Boolean = true,
        customize: MutableMap<KrpcPluginKey, String>.() -> Unit = {},
    ): Unit {
        val pluginParams = mutableMapOf(KrpcPluginKey.MESSAGE_COMPRESSION to codecName)
        if (includeOptionalFields) {
            pluginParams[KrpcPluginKey.MESSAGE_COMPRESSION_ENVELOPE_VERSION] = "1"
            pluginParams[KrpcPluginKey.MESSAGE_COMPRESSION_MAX_SIZE] = "1024"
        }
        pluginParams.customize()

        server.sendMessage(
            KrpcProtocolMessage.Handshake(
                supportedPlugins = KrpcPlugin.ALL + KrpcPlugin.MESSAGE_COMPRESSION,
                pluginParams = pluginParams,
            )
        )
    }
}

private class TestCompressionCodec : KrpcMessageCodec {
    var compressions = 0
        private set

    override val name: String = "test-compression"

    override suspend fun compress(data: ByteArray): ByteArray {
        compressions++
        return data.decodeToString().replace(EXPANDED, TOKEN).encodeToByteArray()
    }

    override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray {
        return data.decodeToString().replace(TOKEN, EXPANDED).encodeToByteArray()
    }

    companion object {
        const val EXPANDED: String = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        private const val TOKEN = "#"
    }
}
