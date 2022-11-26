package dev.twelveoclock.fastutil.set.impl;

import dev.twelveoclock.fastutil.set.base.FastUtilConcurrentSet;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Collection;
import java.util.concurrent.locks.Lock;

import static it.unimi.dsi.fastutil.Hash.DEFAULT_INITIAL_SIZE;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;


public final class ConcurrentObjectOpenCustomHashSet<V> extends FastUtilConcurrentSet implements ObjectSet<V> {

	private final ObjectOpenCustomHashSet<V>[] buckets;

	private final Hash.Strategy<V> strategy;


	public ConcurrentObjectOpenCustomHashSet(final Hash.Strategy<V> strategy) {
		this(Runtime.getRuntime().availableProcessors() - 1, DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR, strategy);
	}

	public ConcurrentObjectOpenCustomHashSet(final int numBuckets, final int loadCapacity, final float loadFactor, final Hash.Strategy<V> strategy) {
		super(numBuckets);

		//noinspection unchecked
		this.buckets = new ObjectOpenCustomHashSet[numBuckets];
		this.strategy = strategy;

		final int bucketLoadCapacity = (int) Math.ceil(((double) loadCapacity) / numBuckets);

		for (int i = 0; i < numBuckets; i++) {
			buckets[i] = new ObjectOpenCustomHashSet<>(bucketLoadCapacity, loadFactor, strategy);
		}
	}


	@Override
	public int size() {

		int size = 0;

		for (final ObjectSet<V> bucket : buckets) {
			size += bucket.size();
		}

		return size;
	}

	@Override
	public boolean isEmpty() {

		for (final ObjectSet<V> bucket : buckets) {
			if (!bucket.isEmpty()) {
				return false;
			}
		}

		return true;
	}


	@Override
	public boolean contains(final Object o) {

		//noinspection unchecked
		final int bucket = getBucket(strategy.hashCode((V) o));

		final Lock readLock = locks[bucket].readLock();
		readLock.lock();

		try {
			return buckets[bucket].contains(o);
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public ObjectIterator<V> iterator() {

		final ObjectOpenHashSet<V> objects = new ObjectOpenHashSet<>(size());

		for (int i = 0; i < buckets.length; i++) {

			final Lock readLock = locks[i].readLock();
			readLock.lock();
			try {
				objects.addAll(buckets[i]);
			}
			finally {
				readLock.unlock();
			}
		}

		return objects.iterator();
	}

	@Override
	public Object[] toArray() {
		return toArray(new Object[size()]);
	}

	@Override
	public <T> T[] toArray(final T[] a) {

		//noinspection unchecked
		final T[] array = a.length >= size() ? a : (T[]) new Object[size()];

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
	public boolean add(final V v) {

		final int bucket = getBucket(strategy.hashCode(v));
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();

		try {
			return buckets[bucket].add(v);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public boolean remove(final Object o) {

		//noinspection unchecked
		final int bucket = getBucket(strategy.hashCode((V) o));
		final Lock writeLock = locks[bucket].writeLock();

		writeLock.lock();

		try {
			return buckets[bucket].remove(o);
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
	public boolean addAll(final Collection<? extends V> c) {

		boolean changed = false;

		for (final V v : c) {
			changed |= add(v);
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
