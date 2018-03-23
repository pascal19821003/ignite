package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;

import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class CacheMetrics {
    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "affi";

    public static void main(String[] args) {
        CacheMetrics test = new CacheMetrics();
        System.out.println("---------------------------" + test.hashCode());
        try (Ignite ignite = Ignition.start("config/AffinityFunction.xml")) {
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);
            int startKey = 0;
            System.out.printf("Usage: " +
                    " 1: add cache." +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while (running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                } else if ("1".equals(command)) {
                    int endKey = startKey + 100;
                    for(;startKey<endKey;startKey++){
                        cache.put(startKey, Integer.toString(startKey));
                    }
                }else if("2".equals(command)){
                    org.apache.ignite.cache.CacheMetrics metrics = cache.metrics();
                    String keyType = metrics.getKeyType();
                    long offHeapAllocatedSize = metrics.getOffHeapAllocatedSize();
                    int totalPartitionsCount = metrics.getTotalPartitionsCount();
                    String name = metrics.name();
                    System.out.println("keyType: " + keyType);
                    System.out.println("offHeapAllocatedSize: " + offHeapAllocatedSize);
                    System.out.println("totalPartitionsCount: " + totalPartitionsCount);
                    System.out.println("name: " + name);
                }
            }
        }
    }


}
