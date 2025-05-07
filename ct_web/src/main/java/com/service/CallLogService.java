package com.service;

import com.model.CallLog;

import java.util.HashMap;
import java.util.List;

public interface CallLogService {
    List<CallLog> getCallLogList(HashMap<String, String> paramsMap);
}
