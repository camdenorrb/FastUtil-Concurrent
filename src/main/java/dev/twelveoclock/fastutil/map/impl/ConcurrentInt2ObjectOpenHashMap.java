package dev.twelveoclock.fastutil.map.impl;

import dev.twelveoclock.fastutil.map.base.FastUtilConcurrentMap;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.locks.Lock;

import static it.unimi.dsi.fastutil.Hash.DEFAULT_INITIAL_SIZE;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;


public final class ConcurrentInt2ObjectOpenHashMap<V> extends FastUtilConcurrentMap implements Int2ObjectMap<V> {

	private final Int2ObjectMap<V>[] buckets;

	@Getter
	private V defaultValue;


	public ConcurrentInt2ObjectOpenHashMap() {
		this(Runtime.getRuntime().availableProcessors() - 1, null, DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	public ConcurrentInt2ObjectOpenHashMap(final int numBuckets, final V defaultValue, final int loadCapacity, final float loadFactor) {

		super(numBuckets);

		//noinspection unchecked
		this.buckets = new Int2ObjectMap[numBuckets];
		this.defaultValue = defaultValue;

		final int bucketLoadCapacity = (int) Math.ceil(((double) loadCapacity) / numBuckets);

		for (int i = 0; i < numBuckets; i++) {
			final Int2ObjectOpenHashMap<V> int2IntOpenHashMap = new Int2ObjectOpenHashMap(bucketLoadCapacity, loadFactor);
			if (defaultValue != null) {
				int2IntOpenHashMap.defaultReturnValue(defaultValue);
			}
			buckets[i] = int2IntOpenHashMap;
		}
	}

	@Override
	public int size() {

		int size = 0;

		for (final Int2ObjectMap<V> bucket : buckets) {
			size += bucket.size();
		}

		return size;
	}

	@Override
	public boolean isEmpty() {

		for (final Int2ObjectMap<V> bucket : buckets) {
			if (!bucket.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void putAll(final Map<? extends Integer, ? extends V> m) {
		for (final Map.Entry<? extends Integer, ? extends V> entry : m.entrySet()) {

			final int bucket = getBucket(entry.getKey());
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
	public V put(final int key, final V value) {

		final int bucket = getBucket(key);
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();
		try {
			return buckets[bucket].put(key, value);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public V remove(final int key) {

		final int bucket = getBucket(key);
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

		for (final Int2ObjectMap<V> bucket : buckets) {
			bucket.defaultReturnValue(rv);
		}
	}

	@Override
	public V defaultReturnValue() {
		return defaultValue;
	}

	@Override
	public ObjectSet<Entry<V>> int2ObjectEntrySet() {

		final Int2ObjectOpenHashMap<V> map = new Int2ObjectOpenHashMap<>(size());

		for (int i = 0; i < buckets.length; i++) {

			final Int2ObjectMap<V> bucket = buckets[i];
			final Lock readLock = locks[i].readLock();

			readLock.lock();
			try {
				map.putAll(bucket);
			} finally {
				readLock.unlock();
			}
		}

		return map.int2ObjectEntrySet();
	}

	@Override
	public IntSet keySet() {

		final IntOpenHashSet keySets = new IntOpenHashSet(buckets.length);

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
	public V get(final int key) {

		final int bucketIndex = getBucket(key);
		final Int2ObjectMap<V> bucket = buckets[bucketIndex];
		final Lock readLock = locks[bucketIndex].readLock();

		readLock.lock();
		try {
			return bucket.get(key);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean containsKey(final int key) {

		final int bucketIndex = getBucket(key);
		final Int2ObjectMap<V> bucket = buckets[bucketIndex];
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

			final Int2ObjectMap<V> bucket = buckets[i];
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

}
