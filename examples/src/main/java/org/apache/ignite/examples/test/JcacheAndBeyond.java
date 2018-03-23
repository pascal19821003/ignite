package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import java.util.Scanner;

/**
 * Created by pascal on 3/5/18.
 */
public class JcacheAndBeyond {
    public static String CACHE_NAME = "default";
    public static void main(String[] args){
        JcacheAndBeyond jcacheAndBeyond = new JcacheAndBeyond();
        System.out.println("---------------------------" + jcacheAndBeyond.hashCode());
        try (Ignite ignite = Ignition.start("config/JcacheAndBeyond.xml")) {
            IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

            // Store keys in cache (values will end up on different cache nodes).
            for (int i = 0; i < 10; i++)
                cache.put(i, Integer.toString(i) + "______________" + jcacheAndBeyond.hashCode());

            for (int i = 0; i < 10; i++)
                System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');


            System.out.printf("Usage: " +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while(running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                }
            }
        }

    }
}
