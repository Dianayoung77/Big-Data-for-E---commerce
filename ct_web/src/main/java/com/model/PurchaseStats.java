package com.model;

import java.io.Serializable;

public class PurchaseStats implements Serializable {
    private String id_date_contact;
    private String id_date_dimension;
    private String id_contact;
    private int order_count;
    private int total_quantity;
    private double total_amount;
    private String user_id;
    private String user_name;
    private String year;
    private String month;
    private String day;

    // Getters and Setters
    public String getId_date_contact() {
        return id_date_contact;
    }

    public void setId_date_contact(String id_date_contact) {
        this.id_date_contact = id_date_contact;
    }

    public String getId_date_dimension() {
        return id_date_dimension;
    }

    public void setId_date_dimension(String id_date_dimension) {
        this.id_date_dimension = id_date_dimension;
    }

    public String getId_contact() {
        return id_contact;
    }

    public void setId_contact(String id_contact) {
        this.id_contact = id_contact;
    }

    public int getOrder_count() {
        return order_count;
    }

    public void setOrder_count(int order_count) {
        this.order_count = order_count;
    }

    public int getTotal_quantity() {
        return total_quantity;
    }

    public void setTotal_quantity(int total_quantity) {
        this.total_quantity = total_quantity;
    }

    public double getTotal_amount() {
        return total_amount;
    }

    public void setTotal_amount(double total_amount) {
        this.total_amount = total_amount;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return "PurchaseStats{" +
                "order_count=" + order_count +
                ", total_quantity=" + total_quantity +
                ", total_amount=" + total_amount +
                ", user_id='" + user_id + '\'' +
                ", user_name='" + user_name + '\'' +
                ", year='" + year + '\'' +
                ", month='" + month + '\'' +
                ", day='" + day + '\'' +
                '}';
    }
}