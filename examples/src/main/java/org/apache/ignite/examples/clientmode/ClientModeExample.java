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

package org.apache.ignite.examples.clientmode;

import org.apache.ignite.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;

import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_START;
import static org.apache.ignite.lifecycle.LifecycleEventType.AFTER_NODE_STOP;

/**
 * This example shows how to provide your own {@link LifecycleBean} implementation
 * will output occurred lifecycle events to the console.
 * <p>
 * This example does not require remote nodes to be started.
 */
public final class ClientModeExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        System.out.println();
        System.out.println(">>> Lifecycle example started.");

        // Create new configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
//        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder() ;
//        Collection< InetSocketAddress> addrs = new ArrayList();
//        for (int i = 47500; i<=47509 ;i++){
//            addrs.add(new InetSocketAddress("pascal", i));
//        }
//
//        ipFinder.registerAddresses(addrs);
////org.apache.ignite.spi.discovery.tcp.ipfinder.zk.TcpDiscoveryZookeeperIpFinder
//
//        tcpDiscoverySpi.setIpFinder(ipFinder);
//
        //peerClassLoadingEnabled
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setClientMode(true);
//        Ignition.setClientMode(true);


        TcpDiscoveryZookeeperIpFinder tcpDiscoveryZookeeperIpFinder = new TcpDiscoveryZookeeperIpFinder();
        tcpDiscoveryZookeeperIpFinder.setZkConnectionString("centosa:2181");
        tcpDiscoverySpi.setIpFinder(tcpDiscoveryZookeeperIpFinder);

        tcpDiscoverySpi.setForceServerMode(true);
        cfg.setDiscoverySpi(tcpDiscoverySpi);

        try (Ignite ignite  = Ignition.start(cfg)) {
//            CacheConfiguration cacheCfg = new CacheConfiguration("myCache");
//            IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheCfg);
//            for(int i=2000;i<3000;i++){
//                cache.put(i,"测试"+i);
//            }
//
//            IgniteCompute compute = ignite.compute();
//            // Execute computation on the server nodes (default behavior).
//            compute.broadcast(() -> System.out.println("Hello Server"));
        }

        Scanner scanner = new Scanner(System.in);
        Boolean running = true;
        System.out.printf("Usage: " +
                " 0: exit the terminal.");
        while(running) {
            String command = scanner.nextLine();
            if ("0".equals(command)) {
                running = false;
            }
        }
    }

}