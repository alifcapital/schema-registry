/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap.Eviction;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class BoundedConcurrentHashMapTest {

  // A map of capacity 4 with one segment batches at most 4 * 10 = 40 cache hits, which is below
  // EvictionPolicy.MAX_BATCH_SIZE, and applies them when the 40th hit is recorded.
  private static final int CAPACITY = 4;
  private static final int BATCH_SIZE = 40;

  private static BoundedConcurrentHashMap<Integer, String> newMap(Eviction eviction) {
    BoundedConcurrentHashMap<Integer, String> map =
        new BoundedConcurrentHashMap<>(CAPACITY, 1, eviction);
    for (int i = 0; i < CAPACITY; i++) {
      map.put(i, "v" + i);
    }
    return map;
  }

  private static int accessQueueLength(BoundedConcurrentHashMap<Integer, String> map)
      throws Exception {
    Object policy = map.segments[0].eviction;
    Field field = policy.getClass().getDeclaredField("accessQueue");
    field.setAccessible(true);
    return ((Collection<?>) field.get(policy)).size();
  }

  private static void hit(BoundedConcurrentHashMap<Integer, String> map, int key, int times) {
    for (int i = 0; i < times; i++) {
      assertEquals("v" + key, map.get(key));
    }
  }

  @Test
  public void lruEvictsLeastRecentlyUsedEntryAtCapacity() {
    BoundedConcurrentHashMap<Integer, String> map = newMap(Eviction.LRU);
    map.get(0);
    map.put(4, "v4");

    assertEquals(CAPACITY, map.size());
    assertEquals("v0", map.get(0));
    assertNull(map.get(1));
    assertEquals("v2", map.get(2));
    assertEquals("v3", map.get(3));
    assertEquals("v4", map.get(4));
  }

  @Test
  public void lirsEvictsOldestEntryAtCapacity() {
    // the fourth entry moves the oldest one to the queue of cold entries, and the fifth entry
    // evicts it
    BoundedConcurrentHashMap<Integer, String> map = newMap(Eviction.LIRS);
    map.put(4, "v4");

    assertEquals(CAPACITY, map.size());
    assertNull(map.get(0));
    assertEquals("v1", map.get(1));
    assertEquals("v2", map.get(2));
    assertEquals("v3", map.get(3));
    assertEquals("v4", map.get(4));
  }

  @Test
  public void sizeStaysWithinCapacity() {
    for (Eviction eviction : new Eviction[] {Eviction.LRU, Eviction.LIRS}) {
      BoundedConcurrentHashMap<Integer, String> map = newMap(eviction);
      for (int i = 0; i < 1000; i++) {
        map.put(i % 37, "x");
        map.get((i * 7) % 37);
        assertTrue(eviction + " " + map.size(), map.size() <= CAPACITY);
      }
    }
  }

  @Test
  public void lruAppliesAccessBatchWhenThresholdIsReached() throws Exception {
    BoundedConcurrentHashMap<Integer, String> map = newMap(Eviction.LRU);
    LinkedHashMap<?, ?> order = (LinkedHashMap<?, ?>) map.segments[0].eviction;
    assertEquals("[v0, v1, v2, v3]", new ArrayList<>(order.values()).toString());

    hit(map, 0, BATCH_SIZE - 1);
    assertEquals(BATCH_SIZE - 1, accessQueueLength(map));
    assertEquals("[v0, v1, v2, v3]", new ArrayList<>(order.values()).toString());

    hit(map, 0, 1);
    assertEquals(0, accessQueueLength(map));
    assertEquals("[v1, v2, v3, v0]", new ArrayList<>(order.values()).toString());
  }

  @Test
  public void lruStartsANewBatchAfterThresholdIsReached() throws Exception {
    checkNewBatchAfterThreshold(Eviction.LRU);
  }

  @Test
  public void lirsStartsANewBatchAfterThresholdIsReached() throws Exception {
    checkNewBatchAfterThreshold(Eviction.LIRS);
  }

  private void checkNewBatchAfterThreshold(Eviction eviction) throws Exception {
    BoundedConcurrentHashMap<Integer, String> map = newMap(eviction);
    for (int batch = 0; batch < 3; batch++) {
      for (int i = 1; i < BATCH_SIZE; i++) {
        hit(map, 0, 1);
        assertEquals(i, accessQueueLength(map));
      }
      hit(map, 0, 1);
      assertEquals(0, accessQueueLength(map));
    }
  }

  @Test
  public void lruRemovedEntriesDoNotCountTowardsThreshold() throws Exception {
    checkRemovedEntriesDoNotCount(Eviction.LRU);
  }

  @Test
  public void lirsRemovedEntriesDoNotCountTowardsThreshold() throws Exception {
    checkRemovedEntriesDoNotCount(Eviction.LIRS);
  }

  private void checkRemovedEntriesDoNotCount(Eviction eviction) throws Exception {
    BoundedConcurrentHashMap<Integer, String> map = newMap(eviction);
    hit(map, 0, BATCH_SIZE - 1);
    assertEquals(BATCH_SIZE - 1, accessQueueLength(map));

    // all the queued hits of the removed entry leave the queue
    assertEquals("v0", map.remove(0));
    assertEquals(0, accessQueueLength(map));

    map.put(0, "v0");
    for (int i = 1; i < BATCH_SIZE; i++) {
      hit(map, 1, 1);
      assertEquals(i, accessQueueLength(map));
    }
    hit(map, 1, 1);
    assertEquals(0, accessQueueLength(map));
  }

  @Test
  public void lruClearStartsANewBatch() throws Exception {
    checkClearStartsANewBatch(Eviction.LRU);
  }

  @Test
  public void lirsClearStartsANewBatch() throws Exception {
    checkClearStartsANewBatch(Eviction.LIRS);
  }

  private void checkClearStartsANewBatch(Eviction eviction) throws Exception {
    BoundedConcurrentHashMap<Integer, String> map = newMap(eviction);
    hit(map, 0, BATCH_SIZE - 1);
    map.clear();
    assertEquals(0, accessQueueLength(map));

    for (int i = 0; i < CAPACITY; i++) {
      map.put(i, "v" + i);
    }
    for (int i = 1; i < BATCH_SIZE; i++) {
      hit(map, 2, 1);
      assertEquals(i, accessQueueLength(map));
    }
    hit(map, 2, 1);
    assertEquals(0, accessQueueLength(map));
  }

  @Test
  public void lruConcurrentUseKeepsMapAndAccessQueueBounded() throws Exception {
    checkConcurrentUse(Eviction.LRU);
  }

  @Test
  public void lirsConcurrentUseKeepsMapAndAccessQueueBounded() throws Exception {
    checkConcurrentUse(Eviction.LIRS);
  }

  private void checkConcurrentUse(Eviction eviction) throws Exception {
    final int threads = 4;
    final BoundedConcurrentHashMap<Integer, String> map = newMap(eviction);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int seed = t;
        futures.add(executor.submit(() -> {
          start.await();
          for (int i = 0; i < 200_000; i++) {
            int key = (i * 31 + seed) % 8;
            if (i % 16 == 0) {
              map.put(key, "v" + key);
            } else if (i % 97 == 0) {
              map.remove(key);
            } else {
              String value = map.get(key);
              assertTrue(value == null || value.equals("v" + key));
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(60, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    assertTrue(map.size() <= CAPACITY);
    // Hits are applied once the queue reaches the batch size, so at the end it can hold at most a
    // batch plus the hits that were racing with the application of the previous batch.
    assertTrue(String.valueOf(accessQueueLength(map)), accessQueueLength(map) <= 2 * BATCH_SIZE);
  }
}
