package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.examples.datagrid.CacheQueryExample;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

/**
 * Created by pascal on 3/5/18.
 */
public class CacheQuery {
    /** Organizations cache name. */
    private static final String ORG_CACHE = "org";

    /** Persons collocated with Organizations cache name. */
    private static final String COLLOCATED_PERSON_CACHE = "CollocatedPersons";

    /** Persons cache name. */
    private static final String PERSON_CACHE = "person";

    public static void main(String[] args){
        CacheQuery test = new CacheQuery();
        System.out.println("---------------------------" + test.hashCode());
        try (Ignite ignite = Ignition.start("config/CacheQuery.xml")) {

            System.out.println();
            System.out.println(">>> Cache query example started.");

//            CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);
//
//            orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
//            orgCacheCfg.setIndexedTypes(Long.class, Organization.class);
//
//            CacheConfiguration<AffinityKey<Long>, Person> colPersonCacheCfg =
//                    new CacheConfiguration<>(COLLOCATED_PERSON_CACHE);
//
//            colPersonCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
//            colPersonCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);
//
//            CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<>(PERSON_CACHE);
//
//            personCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
//            personCacheCfg.setIndexedTypes(Long.class, Person.class);

            try{
                // Create caches.
//                ignite.getOrCreateCache(orgCacheCfg);
//                ignite.getOrCreateCache(colPersonCacheCfg);
//                ignite.getOrCreateCache(personCacheCfg);

                // Store keys in cache (values will end up on different cache nodes).
                initialize();


                scanQuery(ignite);


                textQuery(ignite);

                sqlQueryOnOneTable(ignite);

                sqlQueryWithField(ignite);

                affinityQuery(ignite);

            }catch(Exception e){
                e.printStackTrace();
            }




            System.out.printf("Usage: " +
                    " 0: exit the terminal.");
            Scanner scanner = new Scanner(System.in);
            Boolean running = true;
            while(running) {
                String command = scanner.nextLine();
                if ("0".equals(command)) {
                    running = false;
                }
            }
        }




    }

    private static void affinityQuery(Ignite ignite) {
        IgniteCache<Affinity<Long>, Person> cache = ignite.cache(COLLOCATED_PERSON_CACHE);
        String sql = "from person , \"org\".Organization as o where orgId = o.id and lower(o.name)=lower(?)";
        SqlQuery<Affinity<Long>, Person> apacheIgnite = new SqlQuery<Affinity<Long>, Person>(Person.class, sql).setArgs("ApacheIgnite");
        QueryCursor<Cache.Entry<Affinity<Long>, Person>> query = cache.query(apacheIgnite);
        List<Cache.Entry<Affinity<Long>, Person>> all = query.getAll();
        Iterator<Cache.Entry<Affinity<Long>, Person>> iterator = all.iterator();
        while(iterator.hasNext()){
            Cache.Entry<Affinity<Long>, Person> next = iterator.next();
            Person value = next.getValue();
            System.out.println("value===============" + value);
        }
    }

    private static void sqlQueryWithField(Ignite ignite) {
        IgniteCache<Long, Person> cache = ignite.cache("person");
        SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("select concat(firstName, ' ', lastName) from person");
        FieldsQueryCursor<List<?>> query = cache.query(sqlFieldsQuery);
        List<List<?>> all = query.getAll();
        Iterator<List<?>> iterator = all.iterator();
        while (iterator.hasNext()){
            List<?> next = iterator.next();
            Iterator<?> iterator1 = next.iterator();
            while(iterator1.hasNext()){
                System.out.println("=========================" + iterator1.next());
            }
        }
    }

    private static void sqlQueryOnOneTable(Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.cache(PERSON_CACHE);
        // SQL clause which selects salaries based on range.
        String sql = "salary > ? and salary <= ?";
        QueryCursor<Cache.Entry<Long, Person>> query = cache.query(new SqlQuery<Long, Person>(Person.class, sql).setArgs(1000, 2000));
        List<Cache.Entry<Long, Person>> all = query.getAll();
        Iterator<Cache.Entry<Long, Person>> iterator = all.iterator();
        System.out.println("=======================================");
        while(iterator.hasNext()){
            Cache.Entry<Long, Person> next = iterator.next();
            Person value = next.getValue();
            System.out.println(value);
        }
    }

    private static void textQuery(Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.cache("person");
        TextQuery<Long, Person> bachelor = new TextQuery<>(Person.class, "Bachelor");
        QueryCursor<Cache.Entry<Long, Person>> query = cache.query(bachelor);
        Iterator<Cache.Entry<Long, Person>> iterator = query.iterator();
        while(iterator.hasNext()){
            Cache.Entry<Long, Person> next = iterator.next();
            Long key = next.getKey();
            Person value = next.getValue();
            System.out.println("key: " + key + "   value: " +value);
        }
    }

    private static void scanQuery(Ignite ignite) {
        IgniteCache<Long, Person> cache = ignite.cache("person");

        // Get only keys for persons earning more than 1,000.
        List<Person> values = cache.query(new ScanQuery<Long, Person>(
                        (k, p) -> p.salary > 1000), // Remote filter.
                Cache.Entry::getValue              // Transformer.
        ).getAll();

        for(Person p: values){
            System.out.println(p);
        }

        // Find only persons earning more than 1,000.
        IgniteBiPredicate<Long, Person> filter = new IgniteBiPredicate<Long , Person >() {
            @Override
            public boolean apply(Long key, Person p) {
                return p.salary > 1000;
            }
        };

        try (QueryCursor cursor = cache.query(new ScanQuery(filter))) {
            for (Object p : cursor)
                System.out.println(p.toString());
        }


        IgniteCache<Long, Person> cache1 = ignite.cache("person");

// Find only persons earning more than 1,000.
        try (QueryCursor<Person> persons = cache1.query(new ScanQuery<Long, Person> ((k, p) -> p.salary > 1000), Cache.Entry::getValue) ){
            for (Person p : persons)
                System.out.println(p.toString());

        }
    }


    /**
     * Populate cache with test data.
     */
    private static void initialize() {
        IgniteCache<Long, Organization> orgCache = Ignition.ignite().cache(ORG_CACHE);

        // Clear cache before running the example.
        orgCache.clear();

        // Organizations.
        Organization org1 = new Organization("ApacheIgnite");
        Organization org2 = new Organization("Other");

        orgCache.put(org1.id(), org1);
        orgCache.put(org2.id(), org2);

        IgniteCache<AffinityKey<Long>, Person> colPersonCache = Ignition.ignite().cache(COLLOCATED_PERSON_CACHE);
        IgniteCache<Long, Person> personCache = Ignition.ignite().cache(PERSON_CACHE);

        // Clear caches before running the example.
//        colPersonCache.clear();
        personCache.clear();

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

//        // Note that in this example we use custom affinity key for Person objects
//        // to ensure that all persons are collocated with their organizations.
//        colPersonCache.put(p1.key(), p1);
//        colPersonCache.put(p2.key(), p2);
//        colPersonCache.put(p3.key(), p3);
//        colPersonCache.put(p4.key(), p4);

        // These Person objects are not collocated with their organizations.
        personCache.put(p1.id, p1);
        personCache.put(p2.id, p2);
        personCache.put(p3.id, p3);
        personCache.put(p4.id, p4);

//        CollocatedPersons
        colPersonCache.clear();
        colPersonCache.put(p1.key(), p1);
        colPersonCache.put(p2.key(), p2);
        colPersonCache.put(p3.key(), p3);
        colPersonCache.put(p4.key(), p4);
    }

}
