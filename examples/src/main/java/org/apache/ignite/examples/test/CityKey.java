
package org.apache.ignite.examples.test;

import java.io.Serializable;

/**
 * Created by pascal on 3/7/18.
 */
public class CityKey implements Serializable {
    private static final long serialVersionUID = 0L;
    private Long id;
    private String name   ;

    public CityKey() {
    }

    public CityKey(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean equals(Object o) {
        if(this == o) {
            return true;
        } else if(!(o instanceof CityKey)) {
            return false;
        } else {
            CityKey that = (CityKey)o;
            return this.id == that.id && this.id == that.id && this.name.equals( that.name);
        }
    }

    public int hashCode() {
        int res = this.id.intValue();
        res = 31 * res + this.name.hashCode();
        return res;
    }

    public String toString() {
        return "CityKey [id=" + this.id + ", name=" + this.name+ "]";
    }


}
