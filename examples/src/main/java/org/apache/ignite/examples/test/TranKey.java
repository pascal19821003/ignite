package org.apache.ignite.examples.test;

import java.io.Serializable;

/**
 * Created by pascal on 3/7/18.
 */
public class TranKey implements Serializable {
    private static final long serialVersionUID = 0L;
    private Integer id;
    private String priAcctNo;

    public TranKey() {
    }

    public TranKey(Integer id, String priAcctNo) {
        this.id = id;
        this.priAcctNo = priAcctNo;
    }

    public Integer getId() {
        return this.id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getPriAcctNo() {
        return priAcctNo;
    }

    public void setPriAcctNo(String priAcctNo) {
        this.priAcctNo = priAcctNo;
    }

    public boolean equals(Object o) {
        if(this == o) {
            return true;
        } else if(!(o instanceof TranKey)) {
            return false;
        } else {
            TranKey that = (TranKey)o;
            return this.id == that.id && this.id == that.id && this.priAcctNo.equals( that.priAcctNo);
        }
    }

    public int hashCode() {
        int res = this.id;
        res = 31 * res + this.priAcctNo.hashCode();
        return res;
    }

    public String toString() {
        return "TranKey [id=" + this.id + ", priAcctNo=" + this.priAcctNo+ "]";
    }


}
