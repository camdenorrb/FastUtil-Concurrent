package dev.twelveoclock.fastutil.map.impl;

import dev.twelveoclock.fastutil.map.base.FastUtilConcurrentMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.locks.Lock;

import static it.unimi.dsi.fastutil.Hash.DEFAULT_INITIAL_SIZE;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;


public final class ConcurrentLong2ObjectOpenHashMap<V> extends FastUtilConcurrentMap implements Long2ObjectMap<V> {

	private final Long2ObjectMap<V>[] buckets;

	@Getter
	private V defaultValue;


	public ConcurrentLong2ObjectOpenHashMap() {
		this(Runtime.getRuntime().availableProcessors() - 1, null, DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	public ConcurrentLong2ObjectOpenHashMap(final int numBuckets, final V defaultValue, final int loadCapacity, final float loadFactor) {

		super(numBuckets);

		//noinspection unchecked
		this.buckets = new Long2ObjectMap[numBuckets];
		this.defaultValue = defaultValue;

		final int bucketLoadCapacity = (int) Math.ceil(((double) loadCapacity) / numBuckets);

		for (int i = 0; i < numBuckets; i++) {
			final Long2ObjectMap<V> bucket = new Long2ObjectOpenHashMap<>(bucketLoadCapacity, loadFactor);
			if (defaultValue != null) {
				bucket.defaultReturnValue(defaultValue);
			}
			buckets[i] = bucket;
		}
	}

	@Override
	public int size() {

		int size = 0;

		for (final Long2ObjectMap<V> bucket : buckets) {
			size += bucket.size();
		}

		return size;
	}

	@Override
	public boolean isEmpty() {

		for (final Long2ObjectMap<V> bucket : buckets) {
			if (!bucket.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void putAll(final Map<? extends Long, ? extends V> m) {
		for (final Map.Entry<? extends Long, ? extends V> entry : m.entrySet()) {

			final int bucket = getBucket(entry.getKey().hashCode());
			final Lock writeLock = locks[bucket].writeLock();

			writeLock.lock();
			try {
				buckets[bucket].put(entry.getKey(), entry.getValue());
			} finally {
				writeLock.unlock();
			}
		}
	}

	@Override
	public V put(final long key, final V value) {

		final int bucket = getBucket(Long.hashCode(key));
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();
		try {
			return buckets[bucket].put(key, value);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public V remove(final long key) {

		final int bucket = getBucket(Long.hashCode(key));
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();
		try {
			return buckets[bucket].remove(key);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void defaultReturnValue(final V rv) {

		this.defaultValue = rv;

		for (final Long2ObjectMap<V> bucket : buckets) {
			bucket.defaultReturnValue(rv);
		}
	}

	@Override
	public V defaultReturnValue() {
		return defaultValue;
	}

	@Override
	public ObjectSet<Entry<V>> long2ObjectEntrySet() {

		final Long2ObjectMap<V> map = new Long2ObjectOpenHashMap<>(size());

		for (int i = 0; i < buckets.length; i++) {

			final Long2ObjectMap<V> bucket = buckets[i];
			final Lock readLock = locks[i].readLock();

			readLock.lock();
			try {
				map.putAll(bucket);
			} finally {
				readLock.unlock();
			}
		}

		return map.long2ObjectEntrySet();
	}

	@Override
	public LongSet keySet() {

		final LongSet keySets = new LongOpenHashSet(buckets.length);

		for (int i = 0; i < buckets.length; i++) {

			final Lock readLock = locks[i].readLock();

			readLock.lock();
			try {
				keySets.addAll(buckets[i].keySet());
			} finally {
				readLock.unlock();
			}
		}

		return keySets;
	}

	@Override
	public ObjectCollection<V> values() {

		final ObjectOpenHashSet<V> values = new ObjectOpenHashSet<>();

		for (int i = 0; i < buckets.length; i++) {

			final Lock readLock = locks[i].readLock();

			readLock.lock();
			try {
				values.addAll(buckets[i].values());
			} finally {
				readLock.unlock();
			}
		}

		return values;
	}

	@Override
	public V get(final long key) {

		final int bucketIndex = getBucket(Long.hashCode(key));
		final Long2ObjectMap<V> bucket = buckets[bucketIndex];
		final Lock readLock = locks[bucketIndex].readLock();

		readLock.lock();
		try {
			return bucket.get(key);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean containsKey(final long key) {

		final int bucketIndex = getBucket(Long.hashCode(key));
		final Long2ObjectMap<V> bucket = buckets[bucketIndex];
		final Lock readLock = locks[bucketIndex].readLock();

		readLock.lock();
		try {
			return bucket.containsKey(key);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean containsValue(final Object value) {
		for (int i = 0; i < buckets.length; i++) {

			final Long2ObjectMap<V> bucket = buckets[i];
			final Lock readLock = locks[i].readLock();

			readLock.lock();
			try {
				if (bucket.containsValue(value)) {
					return true;
				}
			} finally {
				readLock.unlock();
			}
		}

		return false;
	}

	@Override
	public void clear() {

		for (int i = 0; i < buckets.length; i++) {

			final Long2ObjectMap<V> bucket = buckets[i];
			final Lock writeLock = locks[i].writeLock();

			writeLock.lock();
			try {
				bucket.clear();
			} finally {
				writeLock.unlock();
			}
		}
	}
}
