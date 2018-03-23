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

package org.apache.ignite.examples.java8.computegrid;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * Demonstrates a simple use of Ignite with reduce closure.
 * <p>
 * This example splits a phrase into collection of words, computes their length on different
 * nodes and then computes total amount of non-whitespaces characters in the phrase.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class ComputeClosureExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Compute closure example started. Ignite Web Console is an interactive configuration wizard, management and monitoring tool that allows you to. ");

            // Execute closure on all cluster nodes.
            Collection<Integer> res = ignite.compute().apply(
                (String word) -> {
                    System.out.println();
                    try {
                        Thread.sleep(1000*10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println(">>> Printing '" + word + "' on this node from ignite job.");

                    // Return number of letters in the word.
                    return word.length();
                },
                // Job parameters. Ignite will create as many jobs as there are parameters.
                Arrays.asList("Count characters using closure Ignite ships with Ignite Web console - a web application that can be deployed on your system environment. It allows configuring all the cluster properties and import schema from a database for integrating with persistence stores. It can connect to the specified database and generate all the required OR-mapping configuration (XML and pure Java) and Java domain model POJOs. The web console also features cluster monitoring functionality (available separately as GridGain plugin) that shows various cache and node metrics as well as CPU and heap usage".split(" "))
            );

            int sum = res.stream().mapToInt(i -> i).sum();

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the cluster).");
        }
    }
}