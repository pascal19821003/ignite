package org.apache.ignite.examples.test;

import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by pascal on 3/8/18.
 */
public class CountryWithAffinity implements Serializable {
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

    @QuerySqlField(orderedGroups = {
            @QuerySqlField.Group(name = "idx_language_dt", order=1)
    })
    private String language;


    @QuerySqlField
    private String language2;

    @QuerySqlField(index = true)
    private String language3;


    @QuerySqlField(orderedGroups = {
            @QuerySqlField.Group(name = "idx_language_dt" , order = 0, descending = true)
    })
    private Timestamp dt;

    @QuerySqlField
    private Timestamp dt2;

    @QuerySqlField(index = true)
    private Timestamp dt3;

    /**
     * Custom cache key to guarantee that country is always collocated with its language.
     */
    private transient AffinityKey<Long> key;

    public CountryWithAffinity(Long id, String name, int age, float salary, String language, String language2,String language3,Timestamp dt, Timestamp dt2, Timestamp dt3) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.salary = salary;
        this.language = language;
        this.language2 = language2;
        this.language3 = language3;
        this.dt = dt;
        this.dt2 = dt2;
        this.dt3 = dt3;
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

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public Timestamp getDt() {
        return dt;
    }

    public void setDt(Timestamp dt) {
        this.dt = dt;
    }

    public String getLanguage2() {
        return language2;
    }

    public void setLanguage2(String language2) {
        this.language2 = language2;
    }

    public Timestamp getDt2() {
        return dt2;
    }

    public void setDt2(Timestamp dt2) {
        this.dt2 = dt2;
    }

    public String getLanguage3() {
        return language3;
    }

    public void setLanguage3(String language3) {
        this.language3 = language3;
    }

    public Timestamp getDt3() {
        return dt3;
    }

    public void setDt3(Timestamp dt3) {
        this.dt3 = dt3;
    }

    public AffinityKey<Long> key(){
        if(key == null){
            key = new AffinityKey<Long>(id, language);
        }
        return key;
    }

    @Override
    public String toString() {
        return "Country{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", salary=" + salary +
                ", language=" + language +
                ", dt=" + dt +
                '}';
    }
}
