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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * 用于udf解析json的数组
 * 输入json数组，输出String数组
 */
public class ArraySplit extends UDF {
    public List<Text> evaluate(Text input) {
        String jsonArrayStr = input.toString();
        try {
            JSONArray arr = JSONArray.parseArray(jsonArrayStr);
            List<Text> l = new ArrayList<>();
            for (Object o : arr) {
                l.add(new Text(JSONObject.toJSONString(o)));
            }
            return l;
        } catch (Exception ex) {
            return null;
        }
    }

    public static void main(String[] args) {
        if (args != null && args.length > 0) {
            List<Text> l = new ArraySplit().evaluate(new Text(args[0]));
            if (l != null && l.size() > 0) {
                for (Text text : l) {
                    System.out.println(text);
                }
            }
        }
    }
}
