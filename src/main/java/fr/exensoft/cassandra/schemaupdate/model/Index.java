package fr.exensoft.cassandra.schemaupdate.model;

import fr.exensoft.cassandra.schemaupdate.model.values.IndexOption;

import java.util.HashMap;
import java.util.Map;

public class Index {
    private String name;
    private Column column;
    private Map<IndexOption, String> options;

    public Index(String name, Column column) {
        this.name = name;
        this.column = column;
        this.options = new HashMap<>();
    }

    public Index setOption(IndexOption indexOption, String value) {
        options.put(indexOption, value);
        return this;
    }

    public Index addOption(IndexOption indexOption) {
        options.put(indexOption, "");
        return this;
    }

    public Index removeOption(IndexOption indexOption) {
        options.remove(indexOption);
        return this;
    }

    public Map<IndexOption, String> getOptions() {
        return options;
    }

    public String getName() {
        return name;
    }

    public Column getColumn() {
        return column;
    }
}
