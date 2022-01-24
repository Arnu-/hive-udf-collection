package me.arnu.hive.udf.json;

import junit.framework.TestCase;
import org.apache.hadoop.io.Text;

import java.util.List;

public class ArraySplitTest extends TestCase {

    public void testEvaluate() {
        Text t = new Text("[" +
                "{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}" +
                ",{\"a\":\"4\",\"b\":\"5\",\"c\":\"6\"}" +
                "]");
        List<Text> l = new ArraySplit().evaluate(t);
        for (Text text : l) {
            System.out.println(text);
        }
    }
}