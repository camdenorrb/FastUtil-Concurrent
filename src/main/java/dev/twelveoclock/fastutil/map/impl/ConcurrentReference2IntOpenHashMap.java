package dev.twelveoclock.fastutil.map.impl;

import dev.twelveoclock.fastutil.map.base.FastUtilConcurrentMap;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;
import java.util.concurrent.locks.Lock;

import static it.unimi.dsi.fastutil.Hash.DEFAULT_INITIAL_SIZE;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;


public final class ConcurrentReference2IntOpenHashMap<T> extends FastUtilConcurrentMap implements Reference2IntMap<T> {

	private final Reference2IntLinkedOpenHashMap<T>[] buckets;

	@Getter
	private int defaultValue;



	public ConcurrentReference2IntOpenHashMap() {
		this(Runtime.getRuntime().availableProcessors() - 1, 0, DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	public ConcurrentReference2IntOpenHashMap(final int numBuckets, final int defaultValue, final int loadCapacity, final float loadFactor) {

		super(numBuckets);

		//noinspection unchecked
		this.buckets = new Reference2IntLinkedOpenHashMap[numBuckets];
		this.defaultValue = defaultValue;

		final int bucketLoadCapacity = (int) Math.ceil(((double) loadCapacity) / numBuckets);

		for (int i = 0; i < numBuckets; i++) {
			final Reference2IntLinkedOpenHashMap<T> bucket = new Reference2IntLinkedOpenHashMap<>(bucketLoadCapacity, loadFactor);
			if (defaultValue != 0) {
				bucket.defaultReturnValue(defaultValue);
			}
			buckets[i] = bucket;
		}
	}


	@Override
	public int size() {

		int size = 0;

		for (final Reference2IntMap<T> bucket : buckets) {
			size += bucket.size();
		}

		return size;
	}

	@Override
	public boolean isEmpty() {

		for (final Reference2IntMap<T> bucket : buckets) {
			if (!bucket.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void putAll(final Map<? extends T, ? extends Integer> m) {
		for (final Map.Entry<? extends T, ? extends Integer> entry : m.entrySet()) {

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
	public int put(final T key, final int value) {

		final int bucket = getBucket(key.hashCode());
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();
		try {
			return buckets[bucket].put(key, value);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public int getInt(final Object key) {

		final int bucketIndex = getBucket(key.hashCode());
		final Reference2IntMap<T> bucket = buckets[bucketIndex];
		final Lock readLock = locks[bucketIndex].readLock();

		readLock.lock();
		try {
			return bucket.getInt(key);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public Integer remove(final Object key) {

		final int bucket = getBucket(key.hashCode());
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();
		try {
			return buckets[bucket].remove(key);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void defaultReturnValue(final int rv) {

		this.defaultValue = rv;

		for (final Reference2IntMap<T> bucket : buckets) {
			bucket.defaultReturnValue(rv);
		}
	}

	@Override
	public int defaultReturnValue() {
		return defaultValue;
	}

	@Override
	public ObjectSet<Entry<T>> reference2IntEntrySet() {

		final Reference2IntMap<T> map = new Reference2IntLinkedOpenHashMap<>(size());

		for (int i = 0; i < buckets.length; i++) {

			final Reference2IntMap<T> bucket = buckets[i];
			final Lock readLock = locks[i].readLock();

			readLock.lock();
			try {
				map.putAll(bucket);
			} finally {
				readLock.unlock();
			}
		}

		return map.reference2IntEntrySet();
	}

	@Override
	public ReferenceSet<T> keySet() {

		final ReferenceOpenHashSet<T> keySets = new ReferenceOpenHashSet<>(buckets.length);

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

	@NonNull
	@Override
	public IntCollection values() {

		final IntOpenHashSet values = new IntOpenHashSet();

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
	public boolean containsKey(final Object key) {

		final int bucketIndex = getBucket(key.hashCode());
		final Reference2IntMap<T> bucket = buckets[bucketIndex];
		final Lock readLock = locks[bucketIndex].readLock();

		readLock.lock();
		try {
			return bucket.containsKey(key);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean containsValue(final int value) {

		for (int i = 0; i < buckets.length; i++) {

			final Reference2IntMap<T> bucket = buckets[i];
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

			final Reference2IntMap<T> bucket = buckets[i];
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
