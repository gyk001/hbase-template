package test.com.jd.hbase.leman.client;

import com.jd.hbase.leman.annotation.Column;
import com.jd.hbase.leman.annotation.RowKey;
import com.jd.hbase.leman.annotation.Table;

/**
 * Created by guoyukun on 2016/8/21.
 */
@Table("club_question_test:qa_answer")
public class TestTableModel {

    @RowKey
    private String id;


    @Column(family = "basic", name = "content")
    private String content;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
