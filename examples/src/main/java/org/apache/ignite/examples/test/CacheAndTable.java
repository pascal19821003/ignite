package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteRunnable;

import javax.cache.Cache;
import java.util.List;
import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class CacheAndTable {
    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "affi";
    private static final String DUMMY_CACHE_NAME = "dummy";

    public static void main(String[] args) {
        CacheAndTable test = new CacheAndTable();
        System.out.println("---------------------------" + test.hashCode());
        try (Ignite ignite = Ignition.start("config/AffinityFunction.xml")) {
            // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
            // will appear in future versions, JDBC and ODBC drivers do not require it already).
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME).setSqlSchema("PUBLIC");

            IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg);

                int startKey = 0;
            System.out.printf("Usage: " +
                    " 1: create table and insert data." +
                    " 2: scan cache return binaryobject." +
                    " 3: query table." +
                    " 4: scan cache return real value type." +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while (running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                } else if ("1".equals(command)) {
                   // create table and insert data
                    createTable(cache);

                } else if ("2".equals(command)) {
                    // scan cache
                    scanCache(ignite);
                } else if("3".equals(command)){
                    // query table
                    queryTable(cache);
                }else if("4".equals(command)){
                    // scan cache return real value type
                    scanCacheReturnRealValueType(ignite);
                }
            }
        }
    }

    private static void createTable(IgniteCache<?, ?> cache ){
            // Create reference City table based on REPLICATED template.
            cache.query(new SqlFieldsQuery(
                    "CREATE TABLE city (id LONG, name VARCHAR, address VARCHAR, PRIMARY KEY(id, name)) WITH \"template=replicated, KEY_TYPE=org.apache.ignite.examples.test.CityKey, value_type=org.apache.ignite.examples.test.City, WRAP_KEY=true \"")).getAll();

            print("Created database objects.");

            SqlFieldsQuery qry = new SqlFieldsQuery("INSERT INTO city (id, name, address) VALUES (?, ?, ?)");

            cache.query(qry.setArgs(1L, "Forest Hill", "SH")).getAll();
            cache.query(qry.setArgs(2L, "Denver", "SH")).getAll();
            cache.query(qry.setArgs(3L, "St. Petersburg", "BJ")).getAll();
    }

    private static void scanCache(Ignite ignite){
        IgniteCache<Object, BinaryObject> cache = ignite.cache("SQL_PUBLIC_CITY").withKeepBinary();
        //withKeepBinary
        // Get only keys for persons earning more than 1,000.
        List<BinaryObject> values = cache.query(new ScanQuery<>(new IgniteBiPredicate<Object, BinaryObject>() {
                    @Override public boolean apply(Object key, BinaryObject val) {
                        System.out.println("key:" +key + " value:" + val);
                        return true;
                    }
                }), // Remote filter.
                Cache.Entry::getValue            // Transformer.
        ).getAll();

        for(BinaryObject p: values){
            System.out.println(p);
//            String name = p.<String>field("name");
//            Long id = p.<Long>field("id");
//            System.out.println("id: " + id + "name: " + name);
        }
    }
    private static void queryTable(IgniteCache<?, ?> cache){
        List<List<?>> res = cache.query(new SqlFieldsQuery(
                "SELECT * FROM City")).getAll();

        print("Query results:");

        for (Object next : res)
            System.out.println(">>>    " + next);


    }

    private static void scanCacheReturnRealValueType(Ignite ignite){
        IgniteCache<CityKey, City> cache = ignite.cache("SQL_PUBLIC_CITY");
        //withKeepBinary
        // Get only keys for persons earning more than 1,000.
        List<City> values = cache.query(new ScanQuery<CityKey, City>(), // Remote filter.
                Cache.Entry::getValue            // Transformer.
        ).getAll();

        for(City p: values){
            System.out.println(p);
        }
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
