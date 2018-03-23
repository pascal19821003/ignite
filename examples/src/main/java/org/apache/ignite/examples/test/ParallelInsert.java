package org.apache.ignite.examples.test;

import com.sun.tools.corba.se.idl.constExpr.Times;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by pascal on 3/8/18.
 */
public class ParallelInsert {
    /**
     * Cache name.
     */
    private static final String COUNTRY_CACHE_NAME = "CountryWithAffinity";
    private static final String TOURISM_CACHE_NAME = "tourist";


    public static void main(String[] args) {
        ParallelInsert test = new ParallelInsert();
        System.out.println("---------------------------" + test.hashCode());
        Ignition.setClientMode(false);
        try (Ignite ignite = Ignition.start("config/CreateTableByAnnotation.xml")) {

            // Initialize atomic sequence.
            final IgniteAtomicSequence seq = ignite.atomicSequence("seqName", 0, true);

            int startKey = 0;
            String useage = "Usage: \n" +
                    " 1: 执行插入和查询操作.\n" +
                    " 2: 广播消息， 在每个node执行查询，查看节点本地的数据.\n" +
                    " 3: 使用Affinity方式查询.\n" +
                    " 4: 使用Affinity方式插入数据.\n" +
                    " 5: 查询所有数据，带有节点信息.\n" +
                    " 6: 批量生成数据，使用广播方式.\n" +
                    " 7: 清理数据.\n" +
                    " 8: HELP.\n" +
                    " 9: 汇总各个节点数据，根据affinity key.\n" +
                    " 10：使用Affinity方式，插入并查询数据，使用复合索引.\n" +
                    " 11：使用Affinity方式，插入并查询数据，使用两个索引.\n" +
                    " 12：使用Affinity方式，插入并查询数据，不使用索引.\n" +
                    " 13：重建表.\n" +
                    " 0: exit the terminal.\n";

            Scanner scanner = new Scanner(System.in);
            Boolean running = true;

            // 1: create cache
            IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = createCache(ignite);

            while (running) {
                System.out.printf(useage);
                String command = scanner.nextLine();
                if ("0".equals(command)) {

                    running = false;

                } else if ("1".equals(command)) {

                    // 执行插入和查询操作
                    System.out.println("执行插入和查询操作");
                    insertAndQuery(ignite, seq, cache);

                } else if ("2".equals(command)) {

                    // 广播消息， 在每个节点执行查询，查看每个节点本地的数据。
                    System.out.println("广播消息， 在每个节点执行查询，查看每个节点本地的数据。");
                    broadcastRun(ignite, cache);

                } else if ("3".equals(command)) {

                    // 使用Affinity方式查询
                    queryByAffinity(ignite, scanner, cache);

                } else if ("4".equals(command)) {

                    // 使用Affinity方式插入数据
                    System.out.println("请输入language字段值(eg: Eng3)：");
                    String languageInput = scanner.nextLine();
                    final String language = languageInput.trim();
                    System.out.println("你输入的language是： " + language);

                    long startTime = System.currentTimeMillis();
                    Long res = insertAndQueryUsingAffinity(ignite, seq, language);
                    long endTime = System.currentTimeMillis();
                    print("统计结果是： " + res + " [耗时：  " + (endTime - startTime) + "]");

                } else if ("5".equals(command)) {

                    //查询所有数据，带有节点信息
                    collectAllDataFromClusterServers(ignite, cache);

                } else if ("6".equals(command)) {

                    //批量生成数据，使用广播方式.
                    print("批量生成数据，使用广播方式.");
                    insertDataBatch(ignite, seq, scanner);

                } else if ("7".equals(command)) {

                    print("清理所有数据据");
                    cache.clear();

                } else if ("8".equals(command)) {

                    continue;

                } else if ("9".equals(command)) {
                    //汇总各个节点数据，根据affinity key
                    print("汇总各个节点数据，根据affinity key");
                    collectGroupedDataFromClusterServers(ignite, cache);

                } else if ("10".equals(command)) {
                    //使用Affinity方式，插入并查询数据，使用复合索引
                    print("使用Affinity方式，插入并查询数据，使用复合索引");

                    System.out.println("请输入language字段值(eg: Eng3)：");
                    String languageInput = scanner.nextLine();
                    final String language = languageInput.trim();
                    System.out.println("你输入的language是： " + language);

                    long startTime = System.currentTimeMillis();
                    Long res = insertAndQueryUsingAffinityAndComposeIndex(ignite, seq, language);
                    long endTime = System.currentTimeMillis();
                    print("统计结果是： " + res + " [耗时：  " + (endTime - startTime) + "]");


                } else if ("11".equals(command)) {
                    //使用Affinity方式，插入并查询数据，使用两个索引.
                    print("使用Affinity方式，插入并查询数据，使用两个索引");

                    System.out.println("请输入language字段值(eg: Eng3)：");
                    String languageInput = scanner.nextLine();
                    final String language = languageInput.trim();
                    System.out.println("你输入的language是： " + language);

                    long startTime = System.currentTimeMillis();
                    Long res = insertAndQueryUsingAffinityAndSeperateIndex(ignite, seq, language);
                    long endTime = System.currentTimeMillis();
                    print("统计结果是： " + res + " [耗时：  " + (endTime - startTime) + "]");


                } else if ("12".equals(command)) {
                    //使用Affinity方式，插入并查询数据，不使用索引
                    print("使用Affinity方式，插入并查询数据，不使用索引");

                    System.out.println("请输入language字段值(eg: Eng3)：");
                    String languageInput = scanner.nextLine();
                    final String language = languageInput.trim();
                    System.out.println("你输入的language是： " + language);

                    long startTime = System.currentTimeMillis();
                    Long res = insertAndQueryUsingAffinityWithoutIndex(ignite, seq, language);
                    long endTime = System.currentTimeMillis();
                    print("统计结果是： " + res + " [耗时：  " + (endTime - startTime) + "]");


                } else if ("13".equals(command)) {
                    // 重建表
                    print("重建表");
                    cache = reCreateCache(ignite);
                }


            }
        }
    }

    private static void insertDataBatch(Ignite ignite, final IgniteAtomicSequence seq, Scanner scanner) {
        System.out.println("请输入总数量：");
        String totalRecordsInput = scanner.nextLine();
        final int totalRecords = Integer.parseInt(totalRecordsInput.trim());
        System.out.println("你输入的总数量是： " + totalRecordsInput);

        ClusterGroup grp = ignite.cluster().forServers();
        final int perRecord = totalRecords / grp.nodes().size();
        IgniteCompute compute = ignite.compute(grp);
        // Print out hello message on remote nodes in the cluster group.
        compute.broadcast(new IgniteRunnable() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public void run() {
                IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(
                        "insert into " + COUNTRY_CACHE_NAME + "(_key, id, name, salary, language, language2, language3, dt, dt2, dt3) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                for (int i = 0; i < perRecord; i++) {
                    // Print ID of remote node on remote node.
                    long id = seq.getAndIncrement();
                    //Long id, String name, int age, float salary, String language, Timestamp dt
                    String language = "Eng" + (1 + (int) (Math.random() * 5));
                    Timestamp dt = new Timestamp(System.currentTimeMillis() - (int) (Math.random() * 24 * 60 * 60 * 1000));
                    CountryWithAffinity country = new CountryWithAffinity(id, "name" + id, 30, 1000l, language, language, language, dt, dt, dt);
                    cache.query(sqlFieldsQuery.setArgs(
                            country.key(), country.getId(), country.getName(), country.getSalary(), country.getLanguage(), country.getLanguage2(), country.getLanguage3(), country.getDt(), country.getDt2(), country.getDt3()));
                }

            }
        });
    }

    private static void insertAndQuery(Ignite ignite, final IgniteAtomicSequence seq, final IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache) {
        long startTime = System.currentTimeMillis();
        Long call = ignite.compute().call(new IgniteCallable<Long>() {

            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public Long call() throws Exception {
                // insert data
                insertData(cache, seq);
                // count total records
                return queryTable(ignite);
            }
        });
        long endTime = System.currentTimeMillis();
        print("统计结果是： " + call + " [耗时：  " + (endTime - startTime) + "]");
    }

    private static void broadcastRun(Ignite ignite, final IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache) {
        IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());
        // Print out hello message on remote nodes in the cluster group.
        compute.broadcast(new IgniteRunnable() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public void run() {
                // Print ID of remote node on remote node.
                System.out.println(">>> Hello Node: " + ignite.cluster().localNode().id());
                String sql = "select id, name, salary, language from " + COUNTRY_CACHE_NAME;
                SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                sqlFieldsQuery.setLocal(true);
                FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
                for (List<?> l : query) {
                    StringBuffer sb = new StringBuffer();
                    for (Object o : l) {
                        sb.append(" " + o.toString());
                    }
                    print(sb.toString());
                }

            }
        });
    }

    private static void queryByAffinity(Ignite ignite, Scanner scanner, IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache) {
        System.out.println("使用Affinity方式查询");
        System.out.println("请输入language字段值(eg: Eng3)：");
        String languageInput = scanner.nextLine();
        final String language = languageInput.trim();
        System.out.println("你输入的language是： " + language);

        ignite.compute().affinityRun(COUNTRY_CACHE_NAME, language, () -> {
            System.out.println("----------------start------------");
            System.out.println("使用Affinity方式查询");
            String sql = "select id, name, salary, language from " + COUNTRY_CACHE_NAME + " where language = ? ";
            SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
            sqlFieldsQuery.setArgs(language);
            sqlFieldsQuery.setLocal(true);
            FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
            for (List<?> l : query) {
                StringBuffer sb = new StringBuffer();
                for (Object o : l) {
                    sb.append(" " + o.toString());
                }
                print(sb.toString());
            }
            System.out.println("----------------end------------");
        });
    }

    private static Long insertAndQueryUsingAffinity(Ignite ignite, final IgniteAtomicSequence seq, final String language) {
        return ignite.compute().affinityCall(COUNTRY_CACHE_NAME, language, new IgniteCallable<Long>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public Long call() throws Exception {
                print("-----------------start------------------");
                // 插入记录
                print("执行插入操作");
                {
                    Long id = seq.getAndIncrement();

                    Timestamp dt = new Timestamp(System.currentTimeMillis());

                    CountryWithAffinity country = new CountryWithAffinity(id, "name" + id, 31, 1000f, language, language, language,
                            dt, dt, dt);
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(
                            "insert into " + COUNTRY_CACHE_NAME + "(_key, id, name, salary, language, language2, language3, dt, dt2, dt3 ) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    cache.query(sqlFieldsQuery.setArgs(
                            country.key(), country.getId(), country.getName(), country.getSalary(), country.getLanguage(), country.getLanguage2(), country.getLanguage3(), country.getDt(), country.getDt2(), country.getDt3()));

                }

                Long res = 0l;
                // 查询相同language的统计信息
                print("执行查询操作");
                {
                    String sql = "select count(1) from " + COUNTRY_CACHE_NAME + " where language = ? ";
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                    sqlFieldsQuery.setLocal(true);
                    sqlFieldsQuery.setArgs(language);

                    Object queryRes = null;
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
                    for (List<?> l : query) {
                        for (Object o : l) {
                            queryRes = o;
                        }
                    }
                    if (res != null) {
                        res = Long.valueOf(queryRes.toString());
                    }
                }
                print("-----------------end------------------");
                return res;
            }
        });
    }

    private static Long insertAndQueryUsingAffinityAndComposeIndex(Ignite ignite, final IgniteAtomicSequence seq, final String language) {
        return ignite.compute().affinityCall(COUNTRY_CACHE_NAME, language, new IgniteCallable<Long>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public Long call() throws Exception {
                print("-----------------start------------------");
                // 插入记录
                print("执行插入操作");
                {
                    Long id = seq.getAndIncrement();
                    Timestamp dt = new Timestamp(System.currentTimeMillis());
                    CountryWithAffinity country = new CountryWithAffinity(id, "name" + id, 31, 1000f, language, language, language, dt, dt, dt);
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(
                            "insert into " + COUNTRY_CACHE_NAME + "(_key, id, name, salary, language, language2, language3, dt, dt2, dt3) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    cache.query(sqlFieldsQuery.setArgs(
                            country.key(), country.getId(), country.getName(), country.getSalary(), country.getLanguage(), country.getLanguage2(), country.getLanguage3(), country.getDt(), country.getDt2(), country.getDt3()));

                }

                Long res = 0l;
                // 查询相同language的统计信息
                print("执行查询操作");
                {
                    String sql = "select count(1) from " + COUNTRY_CACHE_NAME + " where dt >= CURRENT_TIMESTAMP - 0.042  and language = ? ";
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                    sqlFieldsQuery.setLocal(true);
                    sqlFieldsQuery.setArgs(language);

                    Object queryRes = null;
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
                    for (List<?> l : query) {
                        for (Object o : l) {
                            queryRes = o;
                        }
                    }
                    if (res != null) {
                        res = Long.valueOf(queryRes.toString());
                    }
                }
                print("-----------------end------------------");
                return res;
            }
        });
    }


    private static Long insertAndQueryUsingAffinityWithoutIndex(Ignite ignite, final IgniteAtomicSequence seq, final String language) {
        return ignite.compute().affinityCall(COUNTRY_CACHE_NAME, language, new IgniteCallable<Long>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public Long call() throws Exception {
                print("-----------------start------------------");
                // 插入记录
                print("执行插入操作");
                {
                    Long id = seq.getAndIncrement();
                    Timestamp dt = new Timestamp(System.currentTimeMillis());
                    CountryWithAffinity country = new CountryWithAffinity(id, "name" + id, 31, 1000f, language, language, language, dt, dt, dt);
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(
                            "insert into " + COUNTRY_CACHE_NAME + "(_key, id, name, salary, language,  language2,   language3,  dt, dt2, dt3) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    cache.query(sqlFieldsQuery.setArgs(
                            country.key(), country.getId(), country.getName(), country.getSalary(), country.getLanguage(), country.getLanguage2(), country.getLanguage3(), country.getDt(), country.getDt2(), country.getDt3()));

                }

                Long res = 0l;
                // 查询相同language的统计信息
                print("执行查询操作");
                {
                    String sql = "select count(1) from " + COUNTRY_CACHE_NAME + " where dt2 >= CURRENT_TIMESTAMP - 0.042  and language2 = ? ";
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                    sqlFieldsQuery.setLocal(true);
                    sqlFieldsQuery.setArgs(language);

                    Object queryRes = null;
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
                    for (List<?> l : query) {
                        for (Object o : l) {
                            queryRes = o;
                        }
                    }
                    if (res != null) {
                        res = Long.valueOf(queryRes.toString());
                    }
                }
                print("-----------------end------------------");
                return res;
            }
        });
    }


    private static Long insertAndQueryUsingAffinityAndSeperateIndex(Ignite ignite, final IgniteAtomicSequence seq, final String language) {
        return ignite.compute().affinityCall(COUNTRY_CACHE_NAME, language, new IgniteCallable<Long>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public Long call() throws Exception {
                print("-----------------start------------------");
                // 插入记录
                print("执行插入操作");
                {
                    Long id = seq.getAndIncrement();
                    Timestamp dt = new Timestamp(System.currentTimeMillis());
                    CountryWithAffinity country = new CountryWithAffinity(id, "name" + id, 31, 1000f, language, language, language, dt, dt, dt);
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(
                            "insert into " + COUNTRY_CACHE_NAME + "(_key, id, name, salary, language,  language2,   language3,  dt, dt2, dt3) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                    cache.query(sqlFieldsQuery.setArgs(
                            country.key(), country.getId(), country.getName(), country.getSalary(), country.getLanguage(), country.getLanguage2(), country.getLanguage3(), country.getDt(), country.getDt2(), country.getDt3()));

                }

                Long res = 0l;
                // 查询相同language的统计信息
                print("执行查询操作");
                {
                    String sql = "select count(1) from " + COUNTRY_CACHE_NAME + " where dt3 >= CURRENT_TIMESTAMP - 0.042  and language3 = ? ";
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                    sqlFieldsQuery.setLocal(true);
                    sqlFieldsQuery.setArgs(language);

                    Object queryRes = null;
                    IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.cache(COUNTRY_CACHE_NAME);
                    FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
                    for (List<?> l : query) {
                        for (Object o : l) {
                            queryRes = o;
                        }
                    }
                    if (res != null) {
                        res = Long.valueOf(queryRes.toString());
                    }
                }
                print("-----------------end------------------");
                return res;
            }
        });
    }


    private static void collectAllDataFromClusterServers(Ignite ignite, final IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache) {
        //查询所有数据，带有节点信息
        print("查询所有数据，带有节点信息");
        ClusterGroup clusterGroup = ignite.cluster().forServers();
        List<IgniteFuture<List<String>>> futs = new ArrayList();
        for (ClusterNode node : clusterGroup.nodes()) {

            IgniteFuture<List<String>> listIgniteFuture = ignite.compute(ignite.cluster().forNode(node)).callAsync(new IgniteCallable<List<String>>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override
                public List<String> call() throws Exception {
                    List<String> r = new ArrayList();
                    ClusterNode clusterNode = ignite.cluster().localNode();

                    String sql = "select id, name, salary, language from " + COUNTRY_CACHE_NAME;
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                    sqlFieldsQuery.setLocal(true);
                    FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
                    for (List<?> l : query) {
                        StringBuffer sb = new StringBuffer();
                        for (Object o : l) {
                            sb.append(" " + o.toString());
                        }
                        r.add(sb.toString() + " [ node:" + clusterNode.id() + "]");
                    }

                    return r;
                }
            });
            futs.add(listIgniteFuture);
        }

        futs.forEach((listIgniteFuture) -> {
            List<String> objects = listIgniteFuture.get();
            for (String o : objects) {
                print(o);
            }
        });
    }


    private static void collectGroupedDataFromClusterServers(Ignite ignite, final IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache) {
        //查询所有数据，带有节点信息
        print("查询所有数据，带有节点信息");
        ClusterGroup clusterGroup = ignite.cluster().forServers();
        List<IgniteFuture<List<String>>> futs = new ArrayList();
        for (ClusterNode node : clusterGroup.nodes()) {

            IgniteFuture<List<String>> listIgniteFuture = ignite.compute(ignite.cluster().forNode(node)).callAsync(new IgniteCallable<List<String>>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override
                public List<String> call() throws Exception {
                    List<String> r = new ArrayList();
                    ClusterNode clusterNode = ignite.cluster().localNode();

                    String sql = "select language, count(1) num from " + COUNTRY_CACHE_NAME + " group by language";
                    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
                    sqlFieldsQuery.setLocal(true);
                    FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
                    for (List<?> l : query) {
                        StringBuffer sb = new StringBuffer();
                        for (Object o : l) {
                            sb.append(" " + o.toString());
                        }
                        r.add(sb.toString() + " [ node:" + clusterNode.id() + "]");
                    }

                    return r;
                }
            });
            futs.add(listIgniteFuture);
        }

        futs.forEach((listIgniteFuture) -> {
            List<String> objects = listIgniteFuture.get();
            for (String o : objects) {
                print(o);
            }
        });
    }


    private static IgniteCache<AffinityKey<Long>, CountryWithAffinity> createCache(Ignite ignite) {
        IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache1 = ignite.cache(COUNTRY_CACHE_NAME);
        if (cache1 != null) {
            return cache1;
        } else {
            CacheConfiguration<AffinityKey<Long>, CountryWithAffinity> countryCacheCfg = new CacheConfiguration<>(COUNTRY_CACHE_NAME);
            countryCacheCfg.setSqlSchema("PUBLIC");
            countryCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
            countryCacheCfg.setIndexedTypes(AffinityKey.class, CountryWithAffinity.class);

            IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.createCache(countryCacheCfg);
            return cache;

        }
    }


    private static IgniteCache<AffinityKey<Long>, CountryWithAffinity> reCreateCache(Ignite ignite) {
        IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache1 = ignite.cache(COUNTRY_CACHE_NAME);
        if (cache1 != null) {
            cache1.destroy();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        CacheConfiguration<AffinityKey<Long>, CountryWithAffinity> countryCacheCfg = new CacheConfiguration<>(COUNTRY_CACHE_NAME);
        countryCacheCfg.setSqlSchema("PUBLIC");
        countryCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        countryCacheCfg.setIndexedTypes(AffinityKey.class, CountryWithAffinity.class);

        IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache = ignite.createCache(countryCacheCfg);
        return cache;

    }

    private static void insertData(IgniteCache<AffinityKey<Long>, CountryWithAffinity> cache, final IgniteAtomicSequence seq) {
        print("执行插入操作");
        long id = seq.getAndIncrement();
        String language = "Eng" + (1 + (int) (Math.random() * 5));
        Timestamp dt = new Timestamp(System.currentTimeMillis());

        CountryWithAffinity c1 = new CountryWithAffinity(id, "name" + id, 100, 1100f, null, language, language, dt, dt, dt);
        cache.put(c1.key(), c1);
    }


    private static Long queryTable(Ignite ignite) {
        print("执行查询操作");
        IgniteCache<Object, Object> cache = ignite.cache(COUNTRY_CACHE_NAME);
        String sql = "select count(1) from " + COUNTRY_CACHE_NAME;
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(sql);
        FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
        Object res = null;
        for (List<?> l : query) {
            for (Object o : l) {
                res = o;
            }
        }
        if (res != null) {
            return Long.valueOf(res.toString());
        } else {
            return 0l;
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
