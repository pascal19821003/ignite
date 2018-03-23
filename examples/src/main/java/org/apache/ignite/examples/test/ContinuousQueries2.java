package org.apache.ignite.examples.test;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.Scanner;

/**
 * Created by pascal on 3/6/18.
 */
public class ContinuousQueries2 {
    /** Cache name. */
    private static final String CACHE_NAME = "continuous";


    public static void main(String[] args) {
        CacheQuery test = new CacheQuery();
        System.out.println("---------------------------" + test.hashCode());
        try (Ignite ignite = Ignition.start("config/ContinuousQueries.xml")) {
            ;
            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {

                int keyCnt = 20;

                //create new continous query
                ContinuousQuery<Integer, String> qry = new ContinuousQuery();
                qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
                    @Override
                    public boolean apply(Integer key, String value) {
                        return key > 10;
                    }
                }));

                //Callback that is called locally when update notification are received.
                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                    @Override
                    public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> iterable) throws CacheEntryListenerException {
                        for (CacheEntryEvent<? extends Integer, ? extends String> e: iterable)
                            System.out.println("local fileter: Update entry [key=" + e.getKey() + ", value=" + e.getValue() + "]");
                    }
                });


                //This filter will be evaluated remotely on all nodes.
                // Entry that pass this filter will be sent to the caller.
                qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<Integer, String>>() {
                    @Override
                    public CacheEntryEventFilter<Integer, String> create() {
                       return new CacheEntryEventFilter<Integer, String>() {
                           @Override
                           public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> cacheEntryEvent) throws CacheEntryListenerException {
                               System.out.println("remote filter: " + cacheEntryEvent.getKey());
                               return cacheEntryEvent.getKey() > 10;
                           }
                       };
                    }
                });


                // Execute query.
                try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                    // Iterate through existing data.
                    for (Cache.Entry<Integer, String> e : cur)
                        System.out.println("Queried existing entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');

                    // Add a few more keys and watch more query notifications.
//                    for (int i = keyCnt; i < keyCnt + 10; i++)
//                        cache.put(i, Integer.toString(i));

                    // Wait for a while while callback is notified about remaining puts.


                    System.out.printf("Usage: " +
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
        }
    }
}
