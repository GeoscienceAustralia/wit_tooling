Java project to read a parquet file with minimum dependencies

Version info
============

1.0
----
- Initial version using org.apache.parquet : parquet-hadoop : 1.12.1
  
How to run
==========

Requirements
-------------

Java runtime 8+

Execute the sample program
--------------------------

`gradlew run`

Displays the schema and first three records of the sample data file in/sampledata.parquet

List dependencies
-----------------

`gradlew listdependencies`

Displays a list of the required compile-time .jar files

Dependencies
============

The output of `gradlew listdependencies` for org.apache.parquet:parquet-hadoop:1.12.1 should be as follows:

```
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.parquet\parquet-hadoop\1.12.1\b305a14e3ed4644ab4536c8e0412c62797e5e8d6\parquet-hadoop-1.12.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.hadoop\hadoop-common\3.3.1\227027e98079d3f0f24c56f323fe27a129658073\hadoop-common-3.3.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.parquet\parquet-column\1.12.1\4fc53f3336f80f6310aed7a887adab1ef36ce243\parquet-column-1.12.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.parquet\parquet-encoding\1.12.1\ca99828b0aac40b040acf33fcfa0c56f6229ee8b\parquet-encoding-1.12.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.parquet\parquet-common\1.12.1\6d639eaa580668033562bc111984684b335f10a\parquet-common-1.12.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.parquet\parquet-format-structures\1.12.1\d3926a328c5b1942ba704b4e5b5e22e43dca8eb8\parquet-format-structures-1.12.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.parquet\parquet-jackson\1.12.1\e2ebb9160ac1f51cc263dbfe17589b21891a9b68\parquet-jackson-1.12.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.apache.hadoop.thirdparty\hadoop-shaded-guava\1.1.1\2419d851c01139edf9e19b81056382163d9bfab\hadoop-shaded-guava-1.1.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\commons-collections\commons-collections\3.2.2\8ad72fe39fa8c91eaaf12aadb21e0c3661fe26d5\commons-collections-3.2.2.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\log4j\log4j\1.2.17\5af35056b4d257e4b64b9e8069c0746e8b08629f\log4j-1.2.17.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\com.fasterxml.woodstox\woodstox-core\5.3.0\59a3a7fb46a364ee383ea7e8c67c152a224b3d99\woodstox-core-5.3.0.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.codehaus.woodstox\stax2-api\4.2.1\a3f7325c52240418c2ba257b103c3c550e140c83\stax2-api-4.2.1.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.xerial.snappy\snappy-java\1.1.8.2\4205e3cf9c44264731ad002fcd2520eb1b2bb801\snappy-java-1.1.8.2.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\org.slf4j\slf4j-api\1.7.22\a1c83373863cec7ae8d89dc1c5722d8cb6ec0309\slf4j-api-1.7.22.jar
C:\Users\username\.gradle\caches\modules-2\files-2.1\javax.annotation\javax.annotation-api\1.3.2\934c04d3cfef185a8008e7bf34331b79730a9d43\javax.annotation-api-1.3.2.jar
```
