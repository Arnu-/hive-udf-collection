/*

#     __                        
#    /  |  ____ ___  _          
#   / / | / __//   // / /       
#  /_/`_|/_/  / /_//___/        
create @ 2022/1/22                                
*/
package me.arnu.hive.udf.json;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 用于将json的Array数组解析成多条String
 */
public class ArrayExplode extends GenericUDTF {
    private static Logger logger = LoggerFactory.getLogger(ArrayExplode.class);

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        logger.info("初始化");
        List<? extends StructField> inputFieldRef = argOIs.getAllStructFieldRefs();
        if (inputFieldRef == null || inputFieldRef.size() == 0) {
            logger.error("没有输入初始化参数");
            throw new UDFArgumentException("没有参数");
        }
        List<String> cols = new ArrayList<>();
        List<ObjectInspector> i = new ArrayList<>();
        if (inputFieldRef.size() == 1) {
            StructField col = inputFieldRef.get(0);
            logger.info("初始化参数只有一个：{}，{}，{}", col.getFieldName(), col.getFieldComment(), col.getFieldObjectInspector());
            cols.add(col.getFieldName());
            i.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            // i.add(col.getFieldObjectInspector());
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    cols,
                    i);
        } else if (inputFieldRef.size() == 2) {
            StructField col = inputFieldRef.get(1);
            cols.add(col.getFieldName());
            i.add(inputFieldRef.get(0).getFieldObjectInspector());
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    cols,
                    i);
        }
        return super.initialize(argOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        if (objects == null || objects.length == 0) {
            logger.info("未收到数据");
            return;
        }
        logger.info("收到数据条数：{}", objects.length);
        String value = objects[0].toString();
        logger.info("value:{}", value);
        try {
            JSONArray arr = JSONArray.parseArray(value);
            for (Object o : arr) {
                String s = JSONObject.toJSONString(o);
                forward(Collections.singletonList(s));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("发生异常：", ex);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
