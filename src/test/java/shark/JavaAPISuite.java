package shark;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.hive.conf.HiveConf;

import spark.api.java.JavaRDD;
import spark.api.java.function.Function;

import shark.api.java.JavaSharkContext;
import shark.api.java.JavaTableRDD;
import shark.execution.RowWrapper;

import java.io.Serializable;
import java.util.List;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {

    private static final String WAREHOUSE_PATH = CliTestToolkit$.MODULE$.getWarehousePath("JavaAPISuite");
    private static final String METASTORE_PATH = CliTestToolkit$.MODULE$.getMetastorePath("JavaAPISuite");

    private static transient JavaSharkContext sc;

    @BeforeClass
    public static void oneTimeSetUp() {
        sc = new JavaSharkContext("local", "JavaAPISuite");
        SharkEnv.initWithSharkContext(sc.sharkCtx());
        HiveConf hiveconf = sc.sharkCtx().hiveconf();
        hiveconf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE    , WAREHOUSE_PATH);
        hiveconf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
                "jdbc:derby:;databaseName=" + METASTORE_PATH + ";create=true");
    }

    @AfterClass
    public static void oneTimeTearDown() {
        SharkEnv.stop();
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port");
    }

    @Test
    public void selectQuery() {
        String dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt";
        sc.sql("create table shark_selectquery(key int, val string)");
        sc.sql("load data local inpath '" + dataFilePath + "' overwrite into table shark_selectquery");
        List<String> result = sc.sql("select val from shark_selectquery");
        Assert.assertEquals(500, result.size());
        Assert.assertTrue(result.contains("val_407"));
    }

    @Test
    public void sql2rdd() {
        String dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt";
        sc.sql("create table shark_sql2rdd(key int, val string)");
        sc.sql("load data local inpath '" + dataFilePath + "' overwrite into table shark_sql2rdd");

        JavaTableRDD result = sc.sql2rdd("select val from shark_sql2rdd");
        JavaRDD<String> values = result.mapRows(new Function<RowWrapper, String>() {
            @Override
            public String call(RowWrapper x) {
                return x.getString(0);
            }
        });
        Assert.assertEquals(500, values.count());
        Assert.assertTrue(values.collect().contains("val_407"));
    }
}