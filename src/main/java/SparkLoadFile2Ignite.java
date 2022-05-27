/*

Basic example of spark ETL job reading data in csv and loading it into an apache ignite table
This example doesn't use regular load over jdbc ( which would be natural way to do it) but illustrates how to give connection to 3rd party per partition

1/ Data to load
C:\temp>more data.csv
id;parentid;data;date;url
1;1;here are some data record 1;12/21/2021;www.site1.com
2;1;here are some data record 2;12/22/2021;www.site2.com
3;2;here are some data record 3;12/23/2021;www.site3.com
4;2;here are some data record 4;12/24/2021;www.site4.com

2/ execution

( tested with ignite 2.12 )
[frank@fprhel ignite]$ cat server.sh
docker rm -vf fp-ignite

docker run -d --rm \
  --name ignite \
  --net=host \
  ignite:latest
[frank@fprhel ignite]$



C:\frank\SparkLoadFile2Ignite\target>java -cp "SparkLoadFile2Ignite-1.0-SNAPSHOT-jar-with-dependencies.jar;C:\frank\spark-3.2.0-bin-hadoop3.2\jars\*" SparkLoadFile2Ignite

[...]
Schema:
root
 |-- id: integer (nullable = true)
 |-- parent_id: integer (nullable = true)
 |-- data: string (nullable = true)
 |-- dt: string (nullable = true)
 |-- url: string (nullable = true)
[...]
2021-12-03 16:02:49 INFO  DAGScheduler:54 - Job 2 finished: show at SparkLoadFile2Ignite.java:34, took 0,991968 s
+---+---------+---------------------------+----------+-------------+
| id|parent_id|          data             |        dt|          url|
+---+---------+---------------------------+----------+-------------+
|  1|        1|here are some data record 1|12/21/2021|www.site1.com|
|  2|        1|here are some data record 2|12/22/2021|www.site2.com|
|  3|        2|here are some data record 3|12/23/2021|www.site3.com|
|  4|        2|here are some data record 4|12/24/2021|www.site4.com|
+---+---------+---------------------------+----------+-------------+


bash-4.4# /opt/ignite/apache-ignite/bin/sqlline.sh --verbose=true -u "jdbc:ignite:thin://fprhel:10800"
issuing: !connect "jdbc:ignite:thin://fprhel:10800"
Connecting to jdbc:ignite:thin://fprhel:10800
Enter username for jdbc:ignite:thin://fprhel:10800:
Enter password for jdbc:ignite:thin://fprhel:10800:
Connected to: Apache Ignite (version 2.12.0#20220108-sha1:b1289f75)
Driver: Apache Ignite Thin JDBC Driver (version 2.12.0#20220108-sha1:b1289f75)
Autocommit status: true
Transaction isolation: TRANSACTION_REPEATABLE_READ
sqlline version 1.9.0
0: jdbc:ignite:thin://fprhel:10800>
. . . . . . . . . . . . . . . . . . . .> CREATE TABLE IF NOT EXISTS DATA (
. . . . . . . . . . . . . . . . . . . )>   id int,
. . . . . . . . . . . . . . . . . . . )>   parent_id int,
. . . . . . . . . . . . . . . . . . . )>   data varchar,
. . . . . . . . . . . . . . . . . . . )>   dt varchar,
. . . . . . . . . . . . . . . . . . . )>   url varchar,
. . . . . . . . . . . . . . . . . . . )>   PRIMARY KEY (id)
. . . . . . . . . . . . . . . . . . . )> ) WITH "template=replicated";

No rows affected (0.047 seconds)
0: jdbc:ignite:thin://fprhel:10800> select * from DATA;
+----+-----------+------+----+-----+
| ID | PARENT_ID | DATA | DT | URL |
+----+-----------+------+----+-----+
+----+-----------+------+----+-----+
No rows selected (0.228 seconds)
0: jdbc:ignite:thin://fprhel:10800>


AFTER JOB EXECUTION:

No rows selected (0.017 seconds)
0: jdbc:ignite:thin://fprhel:10800> select * from PUBLIC.DATA;
+----+-----------+-----------------------------+------------+---------------+
| ID | PARENT_ID |      DATA                   |     DT     |      URL      |
+----+-----------+-----------------------------+------------+---------------+
| 11 | 1         | here are some data record 1 | 12/21/2021 | www.site1.com |
| 12 | 1         | here are some data record 2 | 12/22/2021 | www.site2.com |
| 13 | 2         | here are some data record 3 | 12/23/2021 | www.site3.com |
| 14 | 2         | here are some data record 4 | 12/24/2021 | www.site4.com |
+----+-----------+-----------------------------+------------+---------------+
4 rows selected (0.045 seconds)
0: jdbc:ignite:thin://fprhel:10800>

 */

import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

import java.util.Iterator;


public class SparkLoadFile2Ignite {

    private static final String FILE_NAME = "C:\\temp\\data.csv";

    //--------------------------------------------------------------

    public static void main(String[] args) {
        SparkLoadFile2Ignite SparkLoadFile2Ignite = new SparkLoadFile2Ignite();
        SparkLoadFile2Ignite.start();
    }

    //--------------------------------------------------------------

    void start() {
        SparkSession spark = SparkSession.builder()
                .appName("SparkLoadFile2Ignite")
                .master("local")
                .getOrCreate();

        Dataset<Row> extractedData = this.extract(spark, FILE_NAME);
        this.load(extractedData);
    }

    //--------------------------------------------------------------

    Dataset<Row> extract(SparkSession spark, String filename) {

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("multiline", false)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", true)
                .load(filename);

        System.out.println("Schema:");
        df.printSchema();

        System.out.println("Data:");
        df.show();
        // create 2 partitions to illustrate later the role of foreachpartition
        return df.repartition(2);
    }

    //--------------------------------------------------------------

    void load(Dataset<Row> dataset) {

        // loop on each partition
        dataset.foreachPartition(partition -> {
            // instanciate one ignite client per partition (so per spark executor )
            IgniteClientUtil igniteClientUtil = new IgniteClientUtil();
            igniteClientUtil.initIgniteClient();
            // process partition's records
            while (partition.hasNext()) {
                Row record = partition.next();
                int id = record.getInt(0);
                int parent_id = record.getInt(1);
                String data = record.getString(2);
                String date = record.getString(3);
                String url = record.getString(4);
                Record line = new Record((long) id + 400, (long) parent_id, data, date, url);
                igniteClientUtil.insert(line);
            }
            igniteClientUtil.closeIgniteClient();
        });
    }
}
