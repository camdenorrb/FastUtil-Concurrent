package dev.twelveoclock.fastutil.set.base;

import lombok.Getter;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public abstract class FastUtilConcurrentSet {


	@Getter
	protected final int numBuckets;

	protected final ReadWriteLock[] locks;


	protected FastUtilConcurrentSet(final int numBuckets) {

		this.numBuckets = numBuckets;
		this.locks = new ReadWriteLock[numBuckets];

		for (int i = 0; i < numBuckets; i++) {
			locks[i] = new ReentrantReadWriteLock();
		}
	}


	protected int getBucket(final int hashCode) {
		return hashCode % numBuckets;
	}

}