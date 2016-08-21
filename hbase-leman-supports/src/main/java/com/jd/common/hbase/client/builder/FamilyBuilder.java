package com.jd.common.hbase.client.builder;

import java.util.ArrayList;
import java.util.List;

public class FamilyBuilder {
    private List<String> familyList = new ArrayList();

    public static FamilyBuilder newInstance() {
        return new FamilyBuilder();
    }

    public FamilyBuilder addFamily(String family) {
        this.familyList.add(family);
        return this;
    }

    public String[] getFamilies() {
        if (this.familyList.size() < 1) {
            return null;
        }
        return (String[]) this.familyList.toArray(new String[this.familyList.size()]);
    }
}
