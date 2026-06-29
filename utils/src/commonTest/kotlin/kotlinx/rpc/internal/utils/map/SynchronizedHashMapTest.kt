/*
 * Copyright 2023-2025 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.internal.utils.map

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [SynchronizedHashMap] is the [RpcInternalConcurrentHashMap] implementation used on every
 * non-JVM target. Its `keys`/`values`/`entries` accessors must hand back a *snapshot* taken
 * under the lock, not a live view of the backing [HashMap]: a live view is invalidated by any
 * subsequent structural modification and throws `ConcurrentModificationException` when iterated,
 * which on a real connection happens whenever a reader iterates while another coroutine mutates
 * the map (e.g. the client shutdown drain over `requestChannels.values`).
 *
 * The class is exercised directly here so the test is deterministic on the JVM test runner, even
 * though the JVM *factory* returns `java.util.concurrent.ConcurrentHashMap`.
 */
class SynchronizedHashMapTest {
    @Test
    fun valuesSnapshotIsSafeToIterateAfterMutation() {
        val map = SynchronizedHashMap<String, Int>()
        map.put("a", 10)
        map.put("b", 20)

        val values = map.values
        map.put("c", 30) // structural modification invalidates a live HashMap.values view

        assertEquals(2, values.size)
        assertEquals(30, values.sum())
    }

    @Test
    fun keysSnapshotIsSafeToIterateAfterMutation() {
        val map = SynchronizedHashMap<String, Int>()
        map.put("a", 1)
        map.put("b", 2)

        val keys = map.keys
        map.put("c", 3)

        assertEquals(setOf("a", "b"), keys.toSet())
    }

    @Test
    fun entriesSnapshotIsSafeToIterateAfterMutation() {
        val map = SynchronizedHashMap<String, Int>()
        map.put("a", 1)
        map.put("b", 2)

        val entries = map.entries
        map.put("c", 3)

        assertEquals(2, entries.size)
        assertEquals(mapOf("a" to 1, "b" to 2), entries.associate { it.key to it.value })
    }
}
