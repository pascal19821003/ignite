package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;

import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class NearCaches2 {
    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "near";

    public static void main(String[] args) {
        NearCaches2 test = new NearCaches2();
        System.out.println("---------------------------" + test.hashCode());
        try (Ignite ignite = Ignition.start("config/NearCaches.xml")) {
            System.out.printf("Usage: " +
                    " 1: add cache." +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while (running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                }
            }
        }
    }
}
