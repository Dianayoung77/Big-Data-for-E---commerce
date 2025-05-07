package com.service.impl;

import com.dao.PurchaseStatsDAO;
import com.model.PurchaseStats;
import com.service.PurchaseStatsService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;

@Service("purchaseStatsService")
public class PurchaseStatsServiceImpl implements PurchaseStatsService {
    @Resource
    private PurchaseStatsDAO purchaseStatsDAO;

    public List<PurchaseStats> getPurchaseStatsList(HashMap<String, String> paramsMap) {
        List<PurchaseStats> list = purchaseStatsDAO.getPurchaseStatsList(paramsMap);
        return list;
    }
}