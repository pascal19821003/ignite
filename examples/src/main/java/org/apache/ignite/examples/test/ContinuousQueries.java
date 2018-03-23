package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.datagrid.CacheContinuousQueryExample;

import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class ContinuousQueries {
    /** Cache name. */
    private static final String CACHE_NAME = "continuous";


    public static void main(String[] args) {
        CacheQuery test = new CacheQuery();
        System.out.println("---------------------------" + test.hashCode());
        try (Ignite ignite = Ignition.start("config/ContinuousQueries.xml")) {
            int startKey = 0;
            try(IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)){
                int keyCnt = startKey + 20;

                for (;startKey<keyCnt;startKey++){
                    cache .put(startKey, Integer.toString(startKey));
                }
            }

            System.out.printf("Usage: " +
                    " 1: add cache." +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while(running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                }else if("1".equals(command)){
                    try(IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME)){

                        int keyCnt = startKey + 1;

                        for ( ;startKey<keyCnt;startKey++){
                            cache .put(startKey, Integer.toString(startKey));
                        }
                    }
                }
            }
        }
    }
}
