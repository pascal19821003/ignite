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

package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Person;

import javax.cache.Cache;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example to showcase DDL capabilities of Ignite's SQL engine.
 * <p>
 * Remote nodes could be started from command line as follows:
 * {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in either same or another JVM.
 */
public class PascalExampleAffinityQuery {
    /** Dummy cache name. */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";
    public static AtomicLong atomicLong = new AtomicLong(1);
    public static int TOTAL_RECORD = 10*10000;
    public static int CONNECTOR_NUM = 100;
    public static AtomicInteger connectorCounter = new AtomicInteger(CONNECTOR_NUM -1);
    public static Date startTime = null;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    @SuppressWarnings({"unused", "ThrowFromFinallyBlock"})
    public static void main(String[] args) throws Exception {
        PascalExampleAffinityQuery pascalExample = new PascalExampleAffinityQuery();
        Ignition.setClientMode(false);
        try (Ignite ignite = Ignition.start("examples/config/zookeeper-config.xml")) {


            // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
            // will appear in future versions, JDBC and ODBC drivers do not require it already).
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME).setSqlSchema("PUBLIC");
            IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg);

            pascalExample.queryData(cache);

            System.out.printf("Usage: " +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while(running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                }else if("1".equals(command)){
                    print("====================start to scan cache ======================");

                    String cacheName = "SQL_PUBLIC_TRAN";
                    IgniteCache<TranKey, Object> tranCache = ignite.cache(cacheName);

                    // Get only keys for persons earning more than 1,000.
                    List<TranKey> values = tranCache.query(new ScanQuery<TranKey, Object>(), // Remote filter.
                            Cache.Entry::getKey              // Transformer.
                    ).getAll();

                    for(Object p: values){
                        System.out.println(p);
                    }
                    print("====================end to scan cache ======================");
                }else if("2".equals(command)){
                    print("====================start to query table ======================");

                    String cacheName = "SQL_PUBLIC_TRAN";
                    IgniteCompute compute = ignite.compute();

                    String priAcctNo = "6222023000000000000152";
                    // This closure will execute on the remote node where
                    // data with the 'key' is located.
                    compute.affinityRun(cacheName, priAcctNo, () -> {
                        //in Java the parameter is switched on via SqlQuery.setLocal(true) or SqlFieldsQuery.setLocal(true) methods.
                        String sql = "SELECT sum(trans_at) FROM tran WHERE pri_acct_no = ? ";
                        print("----------------------本地查需-----------------------");


// Getting a reference to an underlying cache created for City table above.
                        IgniteCache<Long, Object> cityCache = ignite.cache("SQL_PUBLIC_TRAN");

// Querying data from the cluster using a distributed JOIN.
                        SqlFieldsQuery query = new SqlFieldsQuery("SELECT id, t.TRANS_AT, t.PRI_ACCT_NO, t.SETTLE_DT FROM tran t WHERE pri_acct_no = ? ");

                        query.setArgs(priAcctNo);
                        query.setLocal(true);
                        FieldsQueryCursor <List<?>> cursor = cityCache.query(query);

                        Iterator<List<?>> iterator = cursor.iterator();

                        System.out.println("Query result:");

                        while (iterator.hasNext()) {
                            List<?> row = iterator.next();

                            System.out.println(">>>    " + row.get(0) + " " + row.get(1)  + " " + row.get(2) + " " + row.get(3));
                        }



                    });
                    print("====================end to query table ======================");
                }

            }
            print("Cache query DDL example finished.");
        }
    }


    public void queryData( IgniteCache<?, ?> cache ){
        // 执行一条查询，计算耗时
        Date startTime = new Date();
        List<List<?>> res = cache.query(new SqlFieldsQuery(
                "SELECT sum(trans_at) FROM tran WHERE pri_acct_no = '6222023000000000000107'")).getAll();

        print("Query results:");

        for (Object next : res)
            System.out.println(">>>    " + next);
        Date endTime = new Date();
        print("执行查询耗时： "+((endTime.getTime() - startTime.getTime())  ) + "ms.");
    }
    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }
}
