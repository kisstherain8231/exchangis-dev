package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  process string type filed
 *
 */
public class SplitStringTransformer extends Transformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapStringTransformer.class);

    public SplitStringTransformer() {
        setTransformerName("dx_splitString");
        LOGGER.info("init");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        String regex;
        String [] splitArray;
        int targetIndex;
        try {
            if (paras.length != 3) {
                throw new RuntimeException("dx_splitString paras must be 4");
            }

            columnIndex = (Integer) paras[0];
            regex = (String) paras[1];
            targetIndex = Integer.valueOf((String) paras[2]);

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            //如果字段为空，跳过处理
            if (oriValue == null) {
                return record;
            }

            String newValue = "";
            splitArray = oriValue.split(regex);

            if (null == splitArray ) {
                record.setColumn(columnIndex, new StringColumn(""));
            }
            if (splitArray.length == 0) {
                record.setColumn(columnIndex, new StringColumn(""));
            }

            int size = splitArray.length;
            // normal
            if (targetIndex < size) {
                newValue = splitArray[targetIndex];
                record.setColumn(columnIndex, new StringColumn(newValue));
            }
            // large than array size , default value ""
            else {
                record.setColumn(columnIndex, new StringColumn(""));
            }

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
