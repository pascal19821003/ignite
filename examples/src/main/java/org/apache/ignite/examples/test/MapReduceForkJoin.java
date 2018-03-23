package org.apache.ignite.examples.test;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Created by pascal on 3/6/18.
 */
public class MapReduceForkJoin {

    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "near";

    public static void main(String[] args) {
        MapReduceForkJoin test = new MapReduceForkJoin();
        System.out.println("---------------------------" + test.hashCode());
//        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("config/MapReduceForkJoin.xml")) {

            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);

//            IgniteCompute compute = ignite.compute();
//            Integer cnt = compute.execute(CharacterCountTask.class, "Hello Grid Enabled World!");
//
//            System.out.println(">>>>>>>>>>>>>>total number of charaters in the phrase is "+ cnt);

            IgniteCompute compute = ignite.compute();

            compute.execute(new TaskSessionAttributesTask(), null);

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

    private static class CharacterCountTask1 extends ComputeTaskSplitAdapter<String, Integer> {
        /**
         * splite the received string into word
         * creates a child job for each word
         * sends created jobs to other nodes for proecessing
         * @param arg
         * @return
         * @throws IgniteException
         */

        @Override
        protected Collection<? extends ComputeJob> split(int gridSize, String arg) throws IgniteException {
            String[] words = arg.split(" ");
            ArrayList jobs = new ArrayList(words.length);
            for (final String word: words){
                jobs.add(new ComputeJobAdapter() {
                    @Override
                    public Object execute() throws IgniteException {
                        System.out.println("---------word----------"+ word);
                        return word.length();
                    }
                });
            }
            return jobs;
        }

        @Nullable
        @Override
        public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            int sum = 0;
            for (ComputeJobResult res: results){
                sum += res.<Integer>getData();
            }
            return sum;
        }


    }


    /**
     * Task to count non-white-space characters in a phrase.
     */
    private static class CharacterCountTask extends ComputeTaskAdapter<String, Integer> {
        // 1. Splits the received string into to words
        // 2. Creates a child job for each word
        // 3. Sends created jobs to other nodes for processing.
        @Override
        public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            String[] words = arg.split(" ");

            Map<ComputeJob, ClusterNode> map = new HashMap<>(words.length);

            Iterator<ClusterNode> it = subgrid.iterator();

            for (final String word : arg.split(" ")) {
                // If we used all nodes, restart the iterator.
                if (!it.hasNext())
                    it = subgrid.iterator();

                ClusterNode node = it.next();

                map.put(new ComputeJobAdapter() {
                    @Override public Object execute() {
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");

                        // Return number of letters in the word.
                        return word.length();
                    }
                }, node);
            }

            return map;
        }

        @Override
        public Integer reduce(List<ComputeJobResult> results) {
            int sum = 0;

            for (ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }

    /**
     * Task demonstrating distributed task session attributes.
     * Note that task session attributes are enabled only if
     * @ComputeTaskSessionFullSupport annotation is attached.
     */
    @ComputeTaskSessionFullSupport
    private static class TaskSessionAttributesTask extends ComputeTaskSplitAdapter<Object, Object>{
        @Override
        protected Collection<? extends ComputeJob> split(int gridSize, Object arg)  {
            Collection<ComputeJob> jobs = new LinkedList<>();

            // Generate jobs by number of nodes in the grid.
            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter(arg  ) {
                    //
                    @TaskSessionResource
                    private ComputeTaskSession ses;

                    // Auto-injected job context.
                    @JobContextResource
                    private ComputeJobContext jobCtx;

                    @Override
                    public Object execute() throws IgniteException {
                        // Perform STEP1.
                        try {
                            Thread.sleep(1000+((int)(Math.random()*10000)));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println("---------step1--------------");

                        ses.setAttribute(jobCtx.getJobId(), "STEP1");


                        // Wait for other jobs to complete STEP1.
                        for (ComputeJobSibling sibling : ses.getJobSiblings()){
                            try {
                                ses.waitForAttribute(sibling.getJobId(), "STEP1", 0);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        }
                        // Move on to STEP2.
                        System.out.println("---------step2--------------");

                        return null;
                    }
                });
            }
            return jobs;
        }
        @Override
        public Object reduce(List<ComputeJobResult> results) {
            // No-op.
            return null;
        }
    }

}
