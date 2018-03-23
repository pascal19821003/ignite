package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;

import java.util.*;

/**
 * Created by pascal on 3/6/18.
 */
public class CollocateComputeAndData {

    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "near";

    public static void main(String[] args) {
        CollocateComputeAndData test = new CollocateComputeAndData();
        System.out.println("---------------------------" + test.hashCode());
//        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("config/zookeeper-config.xml")) {
            IgniteCompute compute = ignite.compute();
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

                    for( int key = 0;key< startKey;key++){
                        final  int k = key;
                        // this closure will execute on the remote node where
                        // data with the 'key' is localed.
                        compute.affinityRun(CACHE_NAME, k, ()->{
                            // Peek is a local memory lookup
                            System.out.println("key:"+k + " value:"+ cache.localPeek(k));
                        } );
                    }
                } else if("3".equals(command)){

                    List<IgniteFuture<?>> futs = new ArrayList<>();

                    for( int key = 0;key< startKey;key++){
                        final  int k = key;
                        // this closure will execute on the remote node where
                        // data with the 'key' is localed.
                        IgniteFuture<Void> voidIgniteFuture = compute.affinityRunAsync(CACHE_NAME, k, () -> {
                            // Peek is a local memory lookup
                            System.out.println("key:" + k + " value:" + cache.localPeek(k));
                        });

                        futs.add(voidIgniteFuture);
                    }
                    // wait all futures to complete.
//                    futs.stream().forEach(IgniteFuture::get);

                    System.out.println("------------completed!------------");
                } else if ("4".equals(command)){
                }
            }



        }
    }




}
