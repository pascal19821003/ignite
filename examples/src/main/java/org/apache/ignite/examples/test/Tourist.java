package org.apache.ignite.examples.test;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * Created by pascal on 3/8/18.
 */
public class Tourist implements Serializable {
    /** Indexed field. Will be visible for SQL engine. */
    @QuerySqlField (index = true)
    private long id;

    /** Queryable field. Will be visible for SQL engine. */
    @QuerySqlField
    private String name;

    /** Will NOT be visible for SQL engine. */
    private int age;

    /**
     * Indexed field sorted in descending order.
     * Will be visible for SQL engine.
     */
    @QuerySqlField(index = true, descending = true)
    private float salary;

    public Tourist(Long id, String name, int age, float salary) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.salary = salary;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public float getSalary() {
        return salary;
    }

    public void setSalary(float salary) {

        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Tourist{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", salary=" + salary +
                '}';
    }
}
