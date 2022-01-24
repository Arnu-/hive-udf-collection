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
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

public class ArrayExplode extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> inputFieldRef = argOIs.getAllStructFieldRefs();
        if (inputFieldRef == null || inputFieldRef.size() == 0) {
            throw new UDFArgumentException("没有参数");
        }
        List<String> cols = new ArrayList<>();
        List<ObjectInspector> i = new ArrayList<>();
        if (inputFieldRef.size() == 1) {
            StructField col = inputFieldRef.get(0);
            cols.add(col.getFieldName());
            i.add(col.getFieldObjectInspector());
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
            return;
        }
        String value = objects[0].toString();
        try {
            JSONArray arr = JSONArray.parseArray(value);
            for (Object o : arr) {
                Text t = new Text(JSONObject.toJSONString(o));
                forward(t);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
