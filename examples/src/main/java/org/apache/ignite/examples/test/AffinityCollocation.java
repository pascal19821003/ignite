package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteRunnable;

import javax.cache.Cache;
import java.util.*;

/**
 * Created by pascal on 3/6/18.
 */
public class AffinityCollocation {

    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "near";

    public static void main(String[] args) {
        AffinityCollocation test = new AffinityCollocation();
        System.out.println("---------------------------" + test.hashCode());
//        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("config/zookeeper-config.xml")) {

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
                    int endKey = startKey + 10;
                    for(;startKey<endKey;startKey++){
                        cache.put(startKey, Integer.toString(startKey));
                    }
                } else if ("2".equals(command)) {
                    affinityRun(ignite, cache, startKey);

                } else if("3".equals(command)){
                    keyToNodes(ignite, cache, startKey);
                } else if ("4".equals(command)){
                    Affinity<Integer> affinity = ignite.<Integer>affinity(CACHE_NAME);
                    int partitions = affinity.partitions();
                    int partition = affinity.partition(20);
                    int partition1 = affinity.partition(3);
                    System.out.println("partitions: " + partitions);
                    System.out.println("partition: " + partition);
                    System.out.println("partition1: " + partition1);
                    ClusterNode clusterNode = affinity.mapKeyToNode(20);
                    ClusterNode clusterNode1 = affinity.mapKeyToNode(3);
                    System.out.println("clusterNode: " + clusterNode);
                    System.out.println("clusterNode1: " + clusterNode1);
                }
            }



        }
    }

    private static void keyToNodes(Ignite ignite, final IgniteCache<Integer, String> cache, int startKey) {
        ArrayList<Integer> keys = new ArrayList<>(startKey);
        for(int i=0;i<startKey;i++){
            keys.add(i);
        }

        // Map all keys to nodes
        Map<ClusterNode, Collection<Integer>> mapping = ignite.<Integer>affinity(CACHE_NAME).mapKeysToNodes(keys);

        for(Map.Entry<ClusterNode, Collection<Integer>> m: mapping.entrySet()){
            ClusterNode node = m.getKey();
            Collection<Integer> mappedKeys = m.getValue();
            if ( node != null){
                ignite.compute(ignite.cluster().forNode(node)).run(
                        new IgniteRunnable() {
                            @Override
                            public void run() {
                                // Peek is a local memory lookup, however, value should never be null.
                                // as we are co-located with node that has a given key.
                                for (Integer key: mappedKeys){
                                    String s = cache.localPeek(key);
                                    System.out.println("key: " + key + " value:"+ s + "  on node: " + node);
                                }
                            }
                        }
                );
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
