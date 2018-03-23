package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.Cache;
import java.util.List;
import java.util.Scanner;

/**
 * Created by pascal on 3/8/18.
 */
public class CreateTableByAnnotation {


    /**
     * Cache name.
     */
    private static final String CACHE_NAME = "affi";
    private static final String DUMMY_CACHE_NAME = "dummy";
    private static final String COUNTRY_CACHE_NAME = "country";
    private static final String TOURISM_CACHE_NAME = "tourist";

    public static void main(String[] args) {
        CacheAndTable test = new CacheAndTable();
        System.out.println("---------------------------" + test.hashCode());
        try (Ignite ignite = Ignition.start("config/CreateTableByAnnotation.xml")) {
            // Create dummy cache to act as an entry point for SQL queries (new SQL API which do not require this
            // will appear in future versions, JDBC and ODBC drivers do not require it already).
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME).setSqlSchema("PUBLIC");

            IgniteCache<?, ?> cache = ignite.getOrCreateCache(cacheCfg);

            int startKey = 0;
            System.out.printf("Usage: " +
                    " 1: create cache through annotation class" +
                    " 2: create table by ddl" +
                    " 3: query table, return java object." +
                    " 4: query table of insert data, return java object ." +
                    " 5: query table, return row." +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while (running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {

                    running = false;
                } else if ("1".equals(command)) {
                    // 1: create cache through annotation class
                    createCacheWithAnnotationAndAddCache(ignite);

                } else if ("2".equals(command)) {
                    // 2: create table by ddl
                    createCacheWithAnnotationAndInsertData(ignite);
                } else if("3".equals(command)){
                    // 3: query table, return java object.
                    queryTableReturnObject(ignite);
                }else if("4".equals(command)){
                    // 4: query table, return row.
//                    queryTable(ignite);
                    queryTableReturnObjectOfInsertData(ignite);
                } else if("5".equals(command)){
                    // 4: query table, return row.
//                    queryTable(ignite);
                    queryTable(ignite);
                }

            }
        }
    }

    private static void createCacheWithAnnotationAndAddCache(Ignite ignite ){
        CacheConfiguration<Long, Country> countryCacheCfg = new CacheConfiguration<>(COUNTRY_CACHE_NAME);
        countryCacheCfg.setSqlSchema("PUBLIC");
        countryCacheCfg .setCacheMode(CacheMode.PARTITIONED); // Default.
        countryCacheCfg .setIndexedTypes(Long.class, Country.class);

        IgniteCache<Long, Country> cache = ignite.createCache(countryCacheCfg);

        cache.clear();
        Country c1 = new Country(1l, "name1", 100, 1100f);
        Country c2 = new Country(2l, "name2", 100, 2200f);
        Country c3 = new Country(3l, "name3", 100, 2300f);
        Country c4 = new Country(4l, "name4", 100, 1400f);

        cache.put(c1.getId(), c1);
        cache.put(c2.getId(), c2);
        cache.put(c3.getId(), c3);
        cache.put(c4.getId(), c4);
    }

    private static void createCacheWithAnnotationAndInsertData(Ignite ignite ){
        CacheConfiguration<Long, Tourist> touristCacheConfiguration = new CacheConfiguration<>(TOURISM_CACHE_NAME);
        touristCacheConfiguration.setSqlSchema("PUBLIC");
        touristCacheConfiguration.setIndexedTypes(Long.class, Tourist.class);
        touristCacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        IgniteCache<Long, Tourist> cache = ignite.createCache(touristCacheConfiguration);

        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("insert into tourist(_key, id, name, salary) values(?,?,?,?)");
        cache.query(sqlFieldsQuery.setArgs(1l, 1l, "name1", 1000));
        cache.query(sqlFieldsQuery.setArgs(2l, 2l, "name2", 2000));
        cache.query(sqlFieldsQuery.setArgs(3l, 3l, "name3", 3000));

    }


    private static void queryTableReturnObject(Ignite ignite ){
        IgniteCache<Long, Country> cache = ignite.cache(COUNTRY_CACHE_NAME);
        String sql = "from Country";
        QueryCursor<Cache.Entry<Long, Country>> query = cache.query(new SqlQuery<Long, Country>(Country.class, sql));

        for(Cache.Entry<Long, Country> e: query){
            Long key = e.getKey();
            Country value = e.getValue();
            print("key: " + key + " value: " + value);

        }

    }


    private static void queryTableReturnObjectOfInsertData(Ignite ignite ){
        IgniteCache<Long, Tourist> cache = ignite.cache(TOURISM_CACHE_NAME);
        String sql = "from Tourist";
        QueryCursor<Cache.Entry<Long, Tourist>> query = cache.query(new SqlQuery<Long, Tourist>(Tourist.class, sql));

        for(Cache.Entry<Long, Tourist> e: query){
            Long key = e.getKey();
            Tourist value = e.getValue();
            print("key: " + key + " value: " + value);

        }

    }


    private static void queryTable(Ignite ignite ){
        IgniteCache<Object, Object> cache = ignite.cache(COUNTRY_CACHE_NAME);
        String sql = "select concat(name , ' ', CAST(SALARY AS CHAR) ) from country";
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
        FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
        for(List<?> l: query){
            for(Object o: l ){
                print("res: " + o);
            }
        }

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
