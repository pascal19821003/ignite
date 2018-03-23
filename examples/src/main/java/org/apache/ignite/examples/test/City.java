package org.apache.ignite.examples.test;

import java.io.Serializable;

/**
 * Created by pascal on 3/8/18.
 */
public class City  implements Serializable {
    private static final long serialVersionUID = 0L;
//    private Long id;
    private String name;
    private String address;

    /**
     * Required for binary deserialization.
     */
    public City() {
        // No-op.
    }

    /**
     * @param name Organization name.
     */
    public City(String name, String address) {

        this.name = name;
        this.address = address;
    }

//    /**
//     * @param id Organization ID.
//     * @param name Organization name.
//     */
//    public City(long id, String name, String address) {
//        this.id = id;
//        this.name = name;
//        this.address = address;
//    }

//
//    public Long getId() {
//        return id;
//    }
//
//    public void setId(Long id) {
//        this.id = id;
//    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    //    public boolean equals(Object o) {
//        if(this == o) {
//            return true;
//        } else if(!(o instanceof City)) {
//            return false;
//        } else {
//            City that = (City)o;
//            return this.id == that.id && this.id == that.id && this.name.equals( that.name);
//        }
//    }
//
//    public int hashCode() {
//        int res = this.id.intValue();
//        res = 31 * res + this.name.hashCode();
//        return res;
//    }

    public String toString() {
        return "City [id=" + "null.." + ", name=" + this.name+  ", address=" + this.address+ "]";
    }
//    public String toString() {
//        return "City[ name=" + this.name+ "]";
//    }

}
