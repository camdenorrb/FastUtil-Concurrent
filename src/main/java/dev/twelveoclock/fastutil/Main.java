package dev.twelveoclock.fastutil;

import dev.twelveoclock.fastutil.map.impl.ConcurrentReference2IntOpenHashMap;
import dev.twelveoclock.fastutil.set.impl.ConcurrentLongOpenHashSet;
import dev.twelveoclock.fastutil.set.impl.ConcurrentObjectOpenCustomHashSet;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceLists;


public class Main {

	public static void main(final String[] args) {

		//ReferenceLists.synchronize(new ReferenceArrayList<>());

		final ConcurrentReference2IntOpenHashMap<String> map = new ConcurrentReference2IntOpenHashMap<>();

		map.put("Hello", 1);
		map.put("World", 2);

		System.out.println(map.size());

		System.out.println(map.get("Hello"));
		System.out.println(map.get("World"));

		/*
		final ConcurrentObjectOpenCustomHashSet<Integer> concurrentInt2IntMap = new ConcurrentObjectOpenCustomHashSet<>(new Hash.Strategy<>() {
			@Override
			public int hashCode(final Integer o) {
				return o.hashCode();
			}

			@Override
			public boolean equals(final Integer a, final Integer b) {
				return a.equals(b);
			}
		});

		System.out.println("concurrentInt2IntMap.size() = " + concurrentInt2IntMap.size());

		for (int i = 0; i < 1000; i++) {
			concurrentInt2IntMap.add(i);
		}

		System.out.println("concurrentInt2IntMap.size() = " + concurrentInt2IntMap.size());

		concurrentInt2IntMap.forEach(System.out::println);

		System.out.println("concurrentInt2IntMap.size() = " + concurrentInt2IntMap.size());

		System.out.println("concurrentInt2IntMap.contains(500) = " + concurrentInt2IntMap.contains(null));
		for (int i = 0; i < 1000; i++) {
			concurrentInt2IntMap.remove(i);
		}

		System.out.println("concurrentInt2IntMap.size() = " + concurrentInt2IntMap.size());*/
	}

}
