package com.dao;

import com.model.CallLog;

import java.util.HashMap;
import java.util.List;

public interface CallLogDAO {
    List<CallLog> getCallLogList(HashMap<String, String> paramsMap);
}
