package com.service;

import com.model.PurchaseStats;

import java.util.HashMap;
import java.util.List;

public interface PurchaseStatsService {
    List<PurchaseStats> getPurchaseStatsList(HashMap<String, String> paramsMap);
}