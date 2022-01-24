package me.arnu.hive.udf.json;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;

public class ArrayExplodeTest extends TestCase {

    public void testProcess() throws HiveException {
        Text t = new Text("[" +
                "{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}" +
                ",{\"a\":\"4\",\"b\":\"5\",\"c\":\"6\"}" +
                "]");
        new ArrayExplode().process(new Object[]{t});
    }
}