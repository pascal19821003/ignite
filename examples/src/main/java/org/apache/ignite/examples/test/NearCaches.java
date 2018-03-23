package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.examples.model.Person;

import javax.cache.Cache;
import java.util.List;
import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class NearCaches {
    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "near";

    public static void main(String[] args) {
        NearCaches test = new NearCaches();
        System.out.println("---------------------------" + test.hashCode());
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("config/NearCaches.xml")) {
            // Create near-cache configuration for "myCache".
            NearCacheConfiguration<Integer, String> nearCfg = new NearCacheConfiguration<>();

            // Use LRU eviction policy to automatically evict entries
            // from near-cache, whenever it reaches 100_000 in size.
            nearCfg.setNearEvictionPolicy(new LruEvictionPolicy<>(200));

            // Create a distributed cache on server nodes and
            // a near cache on the local node, named "myCache".
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(
                    new CacheConfiguration<Integer, String>(CACHE_NAME), nearCfg);

//            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);
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
                    // Get only keys for persons earning more than 1,000.
                    List<String> values = cache.query(new ScanQuery<Integer, String>(), // Remote filter.
                            Cache.Entry::getValue              // Transformer.
                    ).getAll();

                    for(String p: values){
                        System.out.println(p);
                    }

//                    for(int i=0;i< startKey;i++){
//                        String s = cache.get(i);
//                        System.out.println(s);
//                    }
                }
            }
        }
    }
}
