package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteRunnable;

import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class ExecutorService {
    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "affi";

    public static void main(String[] args) {
        ExecutorService test = new ExecutorService();
        System.out.println("---------------------------" + test.hashCode());
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("config/ExecutorService.xml")) {

            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);

            // Cluster group for nodes where the attribute 'worker' is defined.
            ClusterGroup workerGrp = ignite.cluster().forAttribute("ROLE", "worker");

            // get cluster-enabled executor service.
            java.util.concurrent.ExecutorService exec = ignite.executorService(workerGrp);

            // iterate through all words in the sentence and create jobs.
            for(final String word: "Print words using runnable".split(" ")){
                //execute runnable on  some node.
                exec.submit(new IgniteRunnable() {
                    @Override
                    public void run() {
                        System.out.println("---------------------------------word: " + word);
                    }
                });
            }

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
