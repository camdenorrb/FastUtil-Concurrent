package dev.twelveoclock.fastutil.map.impl;

import dev.twelveoclock.fastutil.map.base.FastUtilConcurrentMap;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import lombok.Getter;
import lombok.NonNull;

import java.util.Map;
import java.util.concurrent.locks.Lock;


public final class ConcurrentInt2IntOpenHashMap extends FastUtilConcurrentMap implements Int2IntMap {

	private final Int2IntMap[] buckets;

	@Getter
	private int defaultValue;


	public ConcurrentInt2IntOpenHashMap(final int numBuckets, final int defaultValue, final int loadCapacity, final float loadFactor) {

		super(numBuckets);

		this.buckets = new Int2IntMap[numBuckets];
		this.defaultValue = defaultValue;

		final int bucketLoadCapacity = (int) Math.ceil(((double) loadCapacity) / numBuckets);

		for (int i = 0; i < numBuckets; i++) {
			final Int2IntOpenHashMap int2IntOpenHashMap = new Int2IntOpenHashMap(bucketLoadCapacity, loadFactor);
			int2IntOpenHashMap.defaultReturnValue(defaultValue);
			buckets[i] = int2IntOpenHashMap;
		}
	}


	@Override
	public int size() {

		int size = 0;

		for (final Int2IntMap bucket : buckets) {
			size += bucket.size();
		}

		return size;
	}

	@Override
	public boolean isEmpty() {

		for (final Int2IntMap bucket : buckets) {
			if (!bucket.isEmpty()) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void putAll(final Map<? extends Integer, ? extends Integer> m) {
		for (final Map.Entry<? extends Integer, ? extends Integer> entry : m.entrySet()) {

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
	public int put(final int key, final int value) {

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
	public int remove(final int key) {

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
	public void defaultReturnValue(final int rv) {

		this.defaultValue = rv;

		for (final Int2IntMap bucket : buckets) {
			bucket.defaultReturnValue(rv);
		}
	}

	@Override
	public int defaultReturnValue() {
		return defaultValue;
	}

	@Override
	public ObjectSet<Entry> int2IntEntrySet() {

		final Int2IntOpenHashMap map = new Int2IntOpenHashMap(size());

		for (int i = 0; i < buckets.length; i++) {

			final Int2IntMap bucket = buckets[i];
			final Lock readLock = locks[i].readLock();

			readLock.lock();
			try {
				map.putAll(bucket);
			} finally {
				readLock.unlock();
			}
		}

		return map.int2IntEntrySet();
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
	public int get(final int key) {

		final int bucketIndex = getBucket(key);
		final Int2IntMap bucket = buckets[bucketIndex];
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
		final Int2IntMap bucket = buckets[bucketIndex];
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

			final Int2IntMap bucket = buckets[i];
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
