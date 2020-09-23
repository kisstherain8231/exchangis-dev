package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import com.alibaba.fastjson.JSON;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  config type : json format {"orgValue":"targetValue"}
 *  map field to a new value
 *
 */
public class MapStringTransformer extends Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapStringTransformer.class);

    public MapStringTransformer() {
        setTransformerName("dx_mapString");
        LOGGER.info("init");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        String orgValueList;
        String destValueList;
        Map<String, String> convertMap = new HashMap<>();
        try {
            if (paras.length != 3) {
                throw new RuntimeException("dx_mapString paras must be 4");
            }

            columnIndex = (Integer) paras[0];
            orgValueList = (String) paras[1];
            destValueList = (String) paras[2];
            String [] orgArray = orgValueList.split("#");
            String [] destArray = destValueList.split("#");
            for (int i = 0; i < orgArray.length; i++) {
                convertMap.put(orgArray[i], destArray[i]);
            }
            LOGGER.info("映射规则为 ", convertMap);
        } catch (Exception e) {
            LOGGER.error("字段转异常 ", e);
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();

            //如果字段为空，跳过处理
            if (oriValue == null) {
                return record;
            }
            String newValue;
            newValue = convertMap.get(oriValue);
            // not config the k-v map, ignore
            if (newValue == null) {
                return record;
            }

            record.setColumn(columnIndex, new StringColumn(newValue));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
