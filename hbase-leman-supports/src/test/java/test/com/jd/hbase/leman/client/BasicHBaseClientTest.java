package test.com.jd.hbase.leman.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.List;


/**
 * Created by guoyukun on 2016/8/21.
 */
public class BasicHBaseClientTest {



    @Test
    public void insertRow() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("club_question_test:qa_answer"));

        String columnFamily = "basic";
        String qualifier = "";
        String rowkey ="tttt";
        String value ="value";

        Put put = new Put(Bytes.toBytes(rowkey));
        //这里指定了10列
//        for (int i = 0; i < 10; i++) {
//            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier + "_" + i),
//                    Bytes.toBytes(value + "_" + i));
//        }
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("content"), Bytes.toBytes("vvvvv"));
        table.put(put);
        table.close();
    }

    @Test
    public void queryForRow() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("club_question_test:qa_answer"));

        String columnFamily = "basic";
        String qualifier = "";
        String rowkey ="tttt";
        String value ="value";

        Get get = new Get(Bytes.toBytes(rowkey));
        get.addFamily(Bytes.toBytes(columnFamily));


        Result result = table.get(get);

        if (result != null && !result.isEmpty()) {
            StringBuffer sb = new StringBuffer("");
            String rowKey = Bytes.toStringBinary(result.getRow());
            sb.append("rowkey：" + rowKey + " :");
            List<Cell> cellList = result.listCells();
            for (Cell cell : cellList) {
                sb.append("column=" + Bytes.toStringBinary(CellUtil.cloneFamily(cell)) + ":"
                        + Bytes.toStringBinary(CellUtil.cloneQualifier(cell)) + " , timestamp=" + cell.getTimestamp()
                        + " , value= " + Bytes.toStringBinary(CellUtil.cloneValue(cell)) + " ");
            }
            sb.append("}" + "\r\n");
            System.out.println(sb.toString());
        }
//
//        boolean ex = result.getExists();
//        Assert.assertEquals(Boolean.TRUE, ex);
        table.close();
    }

    @Test
    public void queryForRow1() throws Exception {

    }

    @Test
    public void queryForRow2() throws Exception {

    }

}