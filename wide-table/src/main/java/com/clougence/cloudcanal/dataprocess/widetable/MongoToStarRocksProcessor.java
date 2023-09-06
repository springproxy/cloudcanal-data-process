package com.clougence.cloudcanal.dataprocess.widetable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * mongo to Str
 * 
 * @author bucketli 2021/11/29 23:07:26
 */
public class MongoToStarRocksProcessor implements CloudCanalProcessorV2 {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {

        CustomData resultData = null;
        try {
            String dataJson = new ObjectMapper().writeValueAsString(data);

            customLogger.info("in custom code. " + dataJson);

            JSONObject jsonOjb = JSON.parseObject(dataJson);
            JSONArray records = jsonOjb.getJSONArray("records");
            JSONObject afterColumnMapObj = records.getJSONObject(0).getJSONObject("afterColumnMap");
            JSONObject customerIdObj = afterColumnMapObj.getJSONObject("customerId");
            String customerIdStr = customerIdObj != null ? customerIdObj.getString("value") : "";
//            String[] columns = customerIdStr.split(",");
            JSONObject targetJsonOjb = JSON.parseObject(customerIdStr);

            for (String columnKey: targetJsonOjb.keySet()) {
                String keyStr = "customerId_" + columnKey;
                JSONObject newObj = new JSONObject();
                newObj.put("sqlType",12);
                newObj.put("fieldName", keyStr);
                newObj.put("value", targetJsonOjb.get(columnKey));
                newObj.put("null",false);
                newObj.put("dbType","varchar(255)");
                newObj.put("updated",false);
                newObj.put("key",false);

                jsonOjb.getJSONArray("records").getJSONObject(0).getJSONObject("afterColumnMap").put(keyStr, newObj);
            }

            JSONObject dataInTime = new JSONObject();
            dataInTime.put("sqlType",12);
            dataInTime.put("fieldName", "dataInTime");
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime time = LocalDateTime.now();
            String localTime = df.format(time);
            dataInTime.put("value", localTime);
            dataInTime.put("null",false);
            dataInTime.put("dbType","varchar(255)");
            dataInTime.put("updated",false);
            dataInTime.put("key",false);
            jsonOjb.getJSONArray("records").getJSONObject(0).getJSONObject("afterColumnMap").put("dataInTime", dataInTime);

            customLogger.info("after transform:" + jsonOjb.toString());

            resultData = JSON.parseObject(jsonOjb.toString(), CustomData.class);
        } catch (Exception e) {
            customLogger.error("in custom code,log data error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }

        List<CustomData> re = new ArrayList<>();
//        re.add(data);
        boolean b = resultData != null ? re.add(resultData) : re.add(data);
        return re;
    }

    @Override
    public void stop() {
        // do nothing
    }
}
