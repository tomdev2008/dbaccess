package com.dajie.core.dbaccess.demo;

public class Person {

    private int id;

    private String name;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("[id=").append(id).append(",name=").append(name).append("]");
        return sb.toString();
    }
}