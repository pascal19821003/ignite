/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;

import java.util.Collection;
import java.util.Scanner;

/**
 * This example demonstrates how to tweak particular settings of Apache Ignite page memory using
 * {@link DataStorageConfiguration} and set up several data regions for different caches with
 * {@link DataRegionConfiguration}.
 * <p>
 * Additional remote nodes can be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} example-data-regions.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which passing
 * {@code examples/config/example-data-regions.xml} configuration to it.
 */
    public class DataRegionsExample {
    /** Name of the default data region defined in 'example-data-regions.xml'. */
    public static final String REGION_DEFAULT = "Default_Region";

    /** Name of the data region that creates a memory region limited by 40 MB with eviction enabled */
    public static final String REGION_40MB_EVICTION = "40MB_Region_Eviction";

    /** Name of the data region that creates a memory region mapped to a memory-mapped file. */
    public static final String REGION_30MB_MEMORY_MAPPED_FILE = "30MB_Region_Swapping";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-data-regions.xml")) {
            System.out.println();
            System.out.println(">>> Data regions example started.");

            /*
             * Preparing configurations for 2 caches that will be bound to the memory region defined by
             * '10MB_Region_Eviction' data region from 'example-data-regions.xml' configuration.
             */
            CacheConfiguration<Integer, Integer> firstCacheCfg = new CacheConfiguration<>("firstCache");

            firstCacheCfg.setDataRegionName(REGION_40MB_EVICTION);
            firstCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            firstCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            CacheConfiguration<Integer, Integer> secondCacheCfg = new CacheConfiguration<>("secondCache");
            secondCacheCfg.setDataRegionName(REGION_40MB_EVICTION);
            secondCacheCfg.setCacheMode(CacheMode.REPLICATED);
            secondCacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

            IgniteCache<Integer, Integer> firstCache = ignite.getOrCreateCache(firstCacheCfg);
            IgniteCache<Integer, Integer> secondCache = ignite.getOrCreateCache(secondCacheCfg);

            System.out.println(">>> Started two caches bound to '" + REGION_40MB_EVICTION + "' memory region.");

            /*
             * Preparing a configuration for a cache that will be bound to the memory region defined by
             * '5MB_Region_Swapping' data region from 'example-data-regions.xml' configuration.
             */
            CacheConfiguration<Integer, Integer> thirdCacheCfg = new CacheConfiguration<>("thirdCache");

            thirdCacheCfg.setDataRegionName(REGION_30MB_MEMORY_MAPPED_FILE);

            IgniteCache<Integer, Integer> thirdCache = ignite.getOrCreateCache(thirdCacheCfg);

            System.out.println(">>> Started a cache bound to '" + REGION_30MB_MEMORY_MAPPED_FILE + "' memory region.");

            /*
             * Preparing a configuration for a cache that will be bound to the default memory region defined by
             * default 'Default_Region' data region from 'example-data-regions.xml' configuration.
             */
            CacheConfiguration<Integer, Integer> fourthCacheCfg = new CacheConfiguration<>("fourthCache");

            IgniteCache<Integer, Integer> fourthCache = ignite.getOrCreateCache(fourthCacheCfg);

            System.out.println(">>> Started a cache bound to '" + REGION_DEFAULT + "' memory region.");

            System.out.println(">>> Destroying caches...");



            // Get the metrics of all the data regions configured on a node.
            Collection<DataRegionMetrics> regionsMetrics = ignite.dataRegionMetrics();

// Print out some of the metrics.
            for (DataRegionMetrics metrics : regionsMetrics) {
                System.out.println(">>> Memory Region Name: " + metrics.getName());
                System.out.println(">>> Allocation Rate: " + metrics.getAllocationRate());
                System.out.println(">>> Fill Factor: " + metrics.getPagesFillFactor());
            }

            ////////////////////////////////////////
            //
//            // Getting metrics.
//            DataStorageMetrics pm = ignite.dataStorageMetrics();
//
//            System.out.println("Fsync duration: " + pm.getLastCheckpointFsyncDuration());
//
//            System.out.println("Data pages: " + pm.getLastCheckpointDataPagesNumber());
//
//            System.out.println("Checkpoint duration:" + pm.getLastCheckpointDuration());

            // ///////////////////////////////////

            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            System.out.printf("Usage: " +
                    " 0: exit the terminal. \n");
            while(running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                }
            }

            firstCache.destroy();
            secondCache.destroy();
            thirdCache.destroy();
            fourthCache.destroy();
        }
    }
}