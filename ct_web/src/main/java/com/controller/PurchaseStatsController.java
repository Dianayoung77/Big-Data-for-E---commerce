package com.controller;

import com.model.PurchaseStats;
import com.model.QueryInfo;
import com.service.PurchaseStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.HashMap;
import java.util.List;

@Controller
@RequestMapping("/purchaseStats")
public class PurchaseStatsController {

    @Autowired
    public PurchaseStatsService purchaseStatsService;

    @RequestMapping("/queryPurchaseStatsList")
    public String queryPurchaseStats(Model model, QueryInfo queryInfo) {
        HashMap<String, String> hashMap = new HashMap<String, String>();
        hashMap.put("user_id", queryInfo.getTelephone());
        hashMap.put("year", queryInfo.getYear());
        hashMap.put("month", queryInfo.getMonth());

        List<PurchaseStats> list = purchaseStatsService.getPurchaseStatsList(hashMap);

        StringBuilder dateSB = new StringBuilder();
        StringBuilder orderCountSB = new StringBuilder();
        StringBuilder totalAmountSB = new StringBuilder();

        for (int i = 0; i < list.size(); i++) {
            PurchaseStats purchaseStats = list.get(i);
            dateSB.append(purchaseStats.getMonth() + "月,");
            orderCountSB.append(purchaseStats.getOrder_count() + ",");
            totalAmountSB.append(purchaseStats.getTotal_amount() + ",");
        }

        dateSB.deleteCharAt(dateSB.length() - 1);
        orderCountSB.deleteCharAt(orderCountSB.length() - 1);
        totalAmountSB.deleteCharAt(totalAmountSB.length() - 1);

        model.addAttribute("user_id", list.get(0).getUser_id());
        model.addAttribute("user_name", list.get(0).getUser_name());
        model.addAttribute("date", dateSB.toString());
        model.addAttribute("order_count", orderCountSB.toString());
        model.addAttribute("total_amount", totalAmountSB.toString());

        return "/PurchaseStatsEchart";
    }
}