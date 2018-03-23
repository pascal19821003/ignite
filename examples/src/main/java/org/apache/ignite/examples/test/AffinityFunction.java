package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteRunnable;

import javax.cache.Cache;
import java.util.List;
import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class AffinityFunction {
    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "affi";

    public static void main(String[] args) {
        AffinityFunction test = new AffinityFunction();
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
                } else if ("2".equals(command)) {
                    affinityRun(ignite,cache,startKey);
                }
            }
        }
    }


    private static void affinityRun(Ignite ignite, final IgniteCache<Integer, String> cache, int startKey) {
        // this runnable will executeInsert on the remote node where
        // data with the given key is located.
        for (int i=0;i< startKey;i++){
            final int key = i;
            ignite.compute().affinityRun(CACHE_NAME, key, new IgniteRunnable(){

                @Override
                public void run() {
                    String s = cache.localPeek(key);
                    System.out.println("key: " + key + " value: " + s );

                }
            });
        }
    }

}
