package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import com.alibaba.fastjson.JSON;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  config type : json format {"orgValue":"targetValue"}
 *
 */
public class ReplaceFieldTransformer extends Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaceFieldTransformer.class);

    public ReplaceFieldTransformer() {
        setTransformerName("dx_replaceField");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        String mapString ;
        Map<String, String> convertMap ;
        try {
            if (paras.length != 2) {
                throw new RuntimeException("dx_replaceField paras must be 4");
            }

            columnIndex = (Integer) paras[0];
            mapString = (String) paras[1];
            convertMap = JSON.parseObject(mapString, HashMap.class);
            LOGGER.info("映射规则为 ", convertMap);
        } catch (Exception e) {
            LOGGER.error("字段转异常 ", e);
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();

            //如果字段为空，跳过replace处理
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
