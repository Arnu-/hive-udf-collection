# hive-udf-collection
自己写的 hive udf 的集合

## 临时UDF载入方式
    add jar hdfs://path-to-jar/hive-udf-collection-1.0-SNAPSHOT.jar;
    create temporary function jsonArrSplit as 'me.arnu.hive.udf.json.ArraySplit';
    create temporary function jsonArrExp as 'me.arnu.hive.udf.json.ArrayExplode';

## json数组处理
### json数组拆分，输出数组
    json split，使用UDF方式实现
    me.arnu.hive.udf.json.ArraySplit

### json数组拆分，输出多行
    json explode，使用UDTF方式实现
    me.arnu.hive.udf.json.ArrayExplode
    
    
    
