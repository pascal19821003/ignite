package org.apache.ignite.examples.datagrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

import java.util.Scanner;

/**
 * Created by pascal on 3/1/18.
 */
public class CacheExample {
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();


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

        }
    }
}
