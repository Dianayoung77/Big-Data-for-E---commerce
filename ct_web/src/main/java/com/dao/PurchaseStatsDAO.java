package com.dao;

import com.model.PurchaseStats;

import java.util.HashMap;
import java.util.List;

public interface PurchaseStatsDAO {
    List<PurchaseStats> getPurchaseStatsList(HashMap<String, String> paramsMap);
}