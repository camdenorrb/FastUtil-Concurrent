package dev.twelveoclock.fastutil.set.impl;

import dev.twelveoclock.fastutil.set.base.FastUtilConcurrentSet;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Collection;
import java.util.concurrent.locks.Lock;

import static it.unimi.dsi.fastutil.Hash.DEFAULT_INITIAL_SIZE;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;

public final class ConcurrentLongOpenHashSet extends FastUtilConcurrentSet implements LongSet {

	private final LongSet[] buckets;


	public ConcurrentLongOpenHashSet() {
		this(Runtime.getRuntime().availableProcessors() - 1, DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
	}

	public ConcurrentLongOpenHashSet(final int numBuckets, final int loadCapacity, final float loadFactor) {
		super(numBuckets);

		this.buckets = new LongOpenHashSet[numBuckets];

		final int bucketLoadCapacity = (int) Math.ceil(((double) loadCapacity) / numBuckets);

		for (int i = 0; i < numBuckets; i++) {
			buckets[i] = new LongOpenHashSet(bucketLoadCapacity, loadFactor);
		}
	}


	@Override
	public int size() {

		int size = 0;

		for (final LongSet bucket : buckets) {
			size += bucket.size();
		}

		return size;
	}

	@Override
	public boolean isEmpty() {

		for (final LongSet bucket : buckets) {
			if (!bucket.isEmpty()) {
				return false;
			}
		}

		return true;
	}


	@Override
	public boolean contains(final Object o) {

		final int bucket = getBucket(o.hashCode());

		final Lock readLock = locks[bucket].readLock();
		readLock.lock();

		try {
			return buckets[bucket].contains(o);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public long[] toLongArray() {

		//noinspection unchecked
		final long[] array = new long[size()];

		int arrayIndex = 0;

		for (int i = 0; i < buckets.length; i++) {

			final Lock readLock = locks[i].readLock();
			final long[] bucketArray;

			readLock.lock();
			try {
				bucketArray = buckets[i].toLongArray();
			} finally {
				readLock.unlock();
			}

			//noinspection SuspiciousSystemArraycopy
			System.arraycopy(bucketArray, 0, array, arrayIndex, bucketArray.length);
			arrayIndex += bucketArray.length;
		}

		return array;
	}

	@Override
	public long[] toArray(final long[] a) {

		final long[] array = a.length >= size() ? a : new long[size()];

		int arrayIndex = 0;

		for (int i = 0; i < buckets.length; i++) {

			final Lock readLock = locks[i].readLock();
			final long[] bucketArray;

			readLock.lock();
			try {
				bucketArray = buckets[i].toLongArray();
			} finally {
				readLock.unlock();
			}

			//noinspection SuspiciousSystemArraycopy
			System.arraycopy(bucketArray, 0, array, arrayIndex, bucketArray.length);
			arrayIndex += bucketArray.length;
		}

		return array;
	}

	@Override
	public boolean addAll(final LongCollection c) {

		boolean changed = false;

		for (final long l : c) {
			changed |= add(l);
		}

		return changed;
	}

	@Override
	public LongIterator iterator() {

		final LongSet longs = new LongOpenHashSet(size());

		for (int i = 0; i < buckets.length; i++) {

			final Lock readLock = locks[i].readLock();
			readLock.lock();
			try {
				longs.addAll(buckets[i]);
			}
			finally {
				readLock.unlock();
			}
		}

		return longs.iterator();
	}

	@Override
	public Object[] toArray() {
		return toArray(new Object[size()]);
	}

	@Override
	public <T> T[] toArray(final T[] a) {

		//noinspection unchecked
		final T[] array = a.length >= size() ? a : (T[]) new Long[size()];

		int arrayIndex = 0;

		for (int i = 0; i < buckets.length; i++) {

			final Lock readLock = locks[i].readLock();
			final Object[] bucketArray;

			readLock.lock();
			try {
				bucketArray = buckets[i].toArray();
			}
			finally {
				readLock.unlock();
			}

			//noinspection SuspiciousSystemArraycopy
			System.arraycopy(bucketArray, 0, array, arrayIndex, bucketArray.length);
			arrayIndex += bucketArray.length;
		}

		return array;
	}

	@Override
	public boolean add(final long v) {

		final int bucket = getBucket(Long.hashCode(v));
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();

		try {
			return buckets[bucket].add(v);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean contains(final long value) {

		final int bucket = getBucket(Long.hashCode(value));
		final Lock readLock = locks[bucket].readLock();

		readLock.lock();

		try {
			return buckets[bucket].contains(value);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean remove(final long v) {

		final int bucket = getBucket(Long.hashCode(v));
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();

		try {
			return buckets[bucket].remove(v);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean containsAll(final Collection<?> c) {

		for (final Object o : c) {
			if (!contains(o)) {
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean addAll(final Collection<? extends Long> c) {

		boolean changed = false;

		for (final long v : c) {
			changed |= add(v);
		}

		return changed;
	}

	@Override
	public boolean containsAll(final LongCollection c) {

		for (final long o : c) {
			if (!contains(o)) {
				return false;
			}
		}

		return true;
	}

	@Override
	public boolean removeAll(final LongCollection c) {

		boolean changed = false;

		for (final long v : c) {
			changed |= remove(v);
		}

		return changed;
	}

	@Override
	public boolean retainAll(final LongCollection c) {

		boolean changed = false;

		for (int i = 0; i < buckets.length; i++) {

			final Lock writeLock = locks[i].writeLock();
			writeLock.lock();

			try {
				changed |= buckets[i].retainAll(c);
			} finally {
				writeLock.unlock();
			}
		}

		return changed;
	}

	@Override
	public boolean retainAll(final Collection<?> c) {

		boolean changed = false;

		for (int i = 0; i < buckets.length; i++) {

			final Lock writeLock = locks[i].writeLock();
			writeLock.lock();

			try {
				changed |= buckets[i].retainAll(c);
			} finally {
				writeLock.unlock();
			}
		}

		return changed;
	}


	@Override
	public boolean removeAll(final Collection<?> c) {

		boolean changed = false;

		for (final Object o : c) {
			changed |= remove(o);
		}

		return changed;
	}

	@Override
	public void clear() {
		for (int i = 0; i < buckets.length; i++) {

			final Lock writeLock = locks[i].writeLock();
			writeLock.lock();

			try {
				buckets[i].clear();
			} finally {
				writeLock.unlock();
			}
		}
	}

}
