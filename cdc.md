```pyspark
%%configure 

{
"conf" : {
        "spark.sql.catalog.s3_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3_catalog.warehouse": "s3://XXXX/iceberg/warehouse/",
        "spark.sql.catalog.s3_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.s3_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.shuffle.partitions": "20",
        "spark.jars": "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.3.0/iceberg-spark-runtime-3.3_2.12-1.3.0.jar,https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.69/bundle-2.20.69.jar,https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.69/url-connection-client-2.20.69.jar"
    } 
}
```

### Initialize a database and a table with two records


```pyspark
spark.sql('CREATE DATABASE IF NOT EXISTS s3_catalog.cdc')
spark.sql('CREATE OR REPLACE TABLE s3_catalog.cdc.test (record_id long, name string, lastname string) using iceberg')
spark.sql('INSERT INTO s3_catalog.cdc.test VALUES (1,"toto", "to"), (2, "tata", "ta");')
spark.sql("SELECT * FROM s3_catalog.cdc.test").show()
```

    +---------+----+--------+
    |record_id|name|lastname|
    +---------+----+--------+
    |        1|toto|      to|
    |        2|tata|      ta|
    +---------+----+--------+

### At this stage the changelog looks correct


```pyspark
def show_full_cdc():
    spark.sql("""
    CALL s3_catalog.system.create_changelog_view(
      table => 'cdc.test',
      changelog_view => 'my_changelog_view'
    )
    """)
    spark.sql("SELECT * FROM my_changelog_view ORDER BY _change_ordinal").show()

def show_first_snapshot_cdc():
    df = spark.sql("SELECT snapshot_id FROM s3_catalog.cdc.test.history WHERE parent_id is NULL")
    first_snapshot_id = df.first()[0]
    spark.sql(f"""
    CALL s3_catalog.system.create_changelog_view(
      table => 'cdc.test',
      changelog_view => 'my_changelog_view',
      options => map('end-snapshot-id', {first_snapshot_id})
    )
    """)
    spark.sql("SELECT * FROM my_changelog_view ORDER BY _change_ordinal").show()
```


```pyspark
show_full_cdc()
```

    +---------+----+--------+------------+---------------+-------------------+
    |record_id|name|lastname|_change_type|_change_ordinal|_commit_snapshot_id|
    +---------+----+--------+------------+---------------+-------------------+
    |        1|toto|      to|      INSERT|              0|7121676719222123744|
    |        2|tata|      ta|      INSERT|              0|7121676719222123744|
    +---------+----+--------+------------+---------------+-------------------+

### Here we modify a record to generate a new snapshot


```pyspark
spark.sql('UPDATE s3_catalog.cdc.test SET name = "titi",lastname="ti" WHERE record_id=1')
```


### Still the CDC is correct whatever it's generated on a single snapshot or the full history


```pyspark
show_full_cdc()
```

    +---------+----+--------+------------+---------------+-------------------+
    |record_id|name|lastname|_change_type|_change_ordinal|_commit_snapshot_id|
    +---------+----+--------+------------+---------------+-------------------+
    |        1|toto|      to|      INSERT|              0|7121676719222123744|
    |        2|tata|      ta|      INSERT|              0|7121676719222123744|
    |        1|titi|      ti|      INSERT|              1|7351451551379972972|
    |        1|toto|      to|      DELETE|              1|7351451551379972972|
    +---------+----+--------+------------+---------------+-------------------+


```pyspark
show_first_snapshot_cdc()
```

    +---------+----+--------+------------+---------------+-------------------+
    |record_id|name|lastname|_change_type|_change_ordinal|_commit_snapshot_id|
    +---------+----+--------+------------+---------------+-------------------+
    |        1|toto|      to|      INSERT|              0|7121676719222123744|
    |        2|tata|      ta|      INSERT|              0|7121676719222123744|
    +---------+----+--------+------------+---------------+-------------------+

## We drop one column from the table 


```pyspark
spark.sql("ALTER TABLE s3_catalog.cdc.test DROP COLUMN name")
```


### Starting from here the dropped column becomes invisible


```pyspark
show_full_cdc()
```


    +---------+--------+------------+---------------+-------------------+
    |record_id|lastname|_change_type|_change_ordinal|_commit_snapshot_id|
    +---------+--------+------------+---------------+-------------------+
    |        1|      to|      INSERT|              0|7121676719222123744|
    |        2|      ta|      INSERT|              0|7121676719222123744|
    |        1|      ti|      INSERT|              1|7351451551379972972|
    |        1|      to|      DELETE|              1|7351451551379972972|
    +---------+--------+------------+---------------+-------------------+


```pyspark
show_first_snapshot_cdc()
```

    +---------+--------+------------+---------------+-------------------+
    |record_id|lastname|_change_type|_change_ordinal|_commit_snapshot_id|
    +---------+--------+------------+---------------+-------------------+
    |        1|      to|      INSERT|              0|7121676719222123744|
    |        2|      ta|      INSERT|              0|7121676719222123744|
    +---------+--------+------------+---------------+-------------------+

### Add the column back with newly inserted data


```pyspark
spark.sql('ALTER TABLE s3_catalog.cdc.test ADD COLUMN name string')
spark.sql('UPDATE s3_catalog.cdc.test SET name = "unknown"')
```


### The changelog now show the column again but only with data inserted in the latest snapshot


```pyspark
show_full_cdc()
```

    +---------+--------+-------+------------+---------------+-------------------+
    |record_id|lastname|   name|_change_type|_change_ordinal|_commit_snapshot_id|
    +---------+--------+-------+------------+---------------+-------------------+
    |        1|      to|   null|      INSERT|              0|7121676719222123744|
    |        2|      ta|   null|      INSERT|              0|7121676719222123744|
    |        1|      ti|   null|      INSERT|              1|7351451551379972972|
    |        1|      to|   null|      DELETE|              1|7351451551379972972|
    |        1|      ti|   null|      DELETE|              2|9125688462177324320|
    |        2|      ta|unknown|      INSERT|              2|9125688462177324320|
    |        1|      ti|unknown|      INSERT|              2|9125688462177324320|
    |        2|      ta|   null|      DELETE|              2|9125688462177324320|
    +---------+--------+-------+------------+---------------+-------------------+

```pyspark
show_first_snapshot_cdc()
```

    +---------+--------+----+------------+---------------+-------------------+
    |record_id|lastname|name|_change_type|_change_ordinal|_commit_snapshot_id|
    +---------+--------+----+------------+---------------+-------------------+
    |        1|      to|null|      INSERT|              0|7121676719222123744|
    |        2|      ta|null|      INSERT|              0|7121676719222123744|
    +---------+--------+----+------------+---------------+-------------------+


### Note that the column remains invisible until we insert value with that newly inserted column