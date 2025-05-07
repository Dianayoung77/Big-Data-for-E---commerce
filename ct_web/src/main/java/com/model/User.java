package com.model;

import java.io.Serializable;
import java.util.Date;

public class User implements Serializable{
    private int id;
    private String username;
    private String password;
    private String email;
    private String role;
    private int status;
    private Date regTime;
    private String regIp;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Date getRegTime() {
        return regTime;
    }

    public void setRegTime(Date regTime) {
        this.regTime = regTime;
    }

    public String getRegIP() {
        return regIp;
    }

    public void setRegIP(String regIp) {
        this.regIp = regIp;
    }
}
