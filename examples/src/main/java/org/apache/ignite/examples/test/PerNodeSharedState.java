package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by pascal on 3/6/18.
 */
public class PerNodeSharedState {

    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "near";

    public static void main(String[] args) {
        PerNodeSharedState test = new PerNodeSharedState();
        System.out.println("---------------------------" + test.hashCode());
//        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("config/zookeeper-config.xml")) {

            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);


            ClusterGroup random = ignite.cluster().forRandom();
            IgniteCompute compute = ignite.compute(random);

            IgniteCallable<Long> job = new IgniteCallable<Long>(){
                @IgniteInstanceResource
                private Ignite ignite;

                @Override
                public Long call() throws Exception {
                     // get a reference to node local
                    ConcurrentMap<String, AtomicLong> nodeLocalMap = ignite.cluster().nodeLocalMap();
                    AtomicLong counter = nodeLocalMap.get("counter");
                    if(counter ==null){
                        AtomicLong old = nodeLocalMap.putIfAbsent("counter", counter = new AtomicLong());;
                        if( old!=null)
                            counter = old;
                    }
                    long l = counter.incrementAndGet();
                    System.out.println("counter>>>>" + l);
                    return l;
                }
            };

            Long call = compute.call(job);
            System.out.println("counter-----------------------" + call);

            Long call2 = compute.call(job);
            System.out.println("counter-----------------------" + call2);


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
                    int endKey = startKey + 10;
                    for(;startKey<endKey;startKey++){
                        cache.put(startKey, Integer.toString(startKey));
                    }
                }
            }



        }
    }



}
