package demo;

import java.sql.Date;
import java.sql.Timestamp;

import sirius.dbaccess.annotation.TableColumn;

public class Geek {

    @TableColumn(name = "ID")
    private int id;

    @TableColumn(name = "NAME")
    private String name;

    @TableColumn(name = "STATUS")
    private int status;

    @TableColumn(name = "HEIGHT")
    private double height;

    @TableColumn(name = "WEIGHT")
    private float weight;

    @TableColumn(name = "BIRTH")
    private Date birth;

    @TableColumn(name = "LAST_LOGIN")
    private Timestamp lastLogin;

    @TableColumn(name = "SCORE")
    private long score;

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

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public Date getBirth() {
        return birth;
    }

    public void setBirth(Date birth) {
        this.birth = birth;
    }

    public Timestamp getLastLogin() {
        return lastLogin;
    }

    public void setLastLogin(Timestamp lastLogin) {
        this.lastLogin = lastLogin;
    }

    public long getScore() {
        return score;
    }

    public void setScore(long score) {
        this.score = score;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(id).append("|").append(name).append("|").append(height).append("|")
                .append(weight).append("|").append(birth).append("|").append(lastLogin).append("|")
                .append(score);
        return sb.toString();
    }
}
