package test.com.jd.hbase.leman.client;

import com.jd.hbase.leman.client.HBaseTemplate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * Created by guoyukun on 2016/8/21.
 */
@ContextConfiguration("/spring-hbase-test.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class HBaseTemplateTest {


    @Autowired
    private HBaseTemplate hBaseTemplate;


    @Test
    public void insertRow() throws Exception {
        TestTableModel query = new TestTableModel();
        query.setId("tttt");

        TestTableModel obj = hBaseTemplate.queryForRow(query, TestTableModel.class);
        System.out.println(obj.getContent());
        System.out.println(obj.getId());
//
//
//        Table table = connection.getTable(TableName.valueOf("club_question_test:qa_answer"));
//
//        String columnFamily = "basic";
//        String qualifier = "";
//        String rowkey ="tttt";
//        String value ="value";
//
//        Put put = new Put(Bytes.toBytes(rowkey));
//        //这里指定了10列
//        for (int i = 0; i < 10; i++) {
//            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier + "_" + i),
//                    Bytes.toBytes(value + "_" + i));
//        }
//        table.put(put);
//        table.close();
    }

    @Test
    public void queryForRow() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseTemplate template = new HBaseTemplate(conf);
        TestTableModel query = new TestTableModel();
        query.setId("tttt");
        TestTableModel obj = template.queryForRow(query, TestTableModel.class);
        System.out.println(obj.getContent());
        System.out.println(obj.getId());
//
//        boolean ex = result.getExists();
//        Assert.assertEquals(Boolean.TRUE, ex);
    }

    @Test
    public void queryForRow1() throws Exception {

    }

    @Test
    public void queryForRow2() throws Exception {

    }

}