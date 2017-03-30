package fr.exensoft.cassandra.schemaupdate.model;

import fr.exensoft.cassandra.schemaupdate.model.type.ColumnType;

import java.util.ArrayList;
import java.util.List;

public class Column {

    private String name;
    private ColumnType type;

    //Old name of the column (allow to rename the column during update instead of drop/create)
    private List<String> oldNames = new ArrayList<>();

    private int index;
    private int innerIndex;

    public Column(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }

    public List<String> getOldNames() {
        return oldNames;
    }

    public Column addOldName(String oldName) {
        oldNames.add(oldName);
        return this;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getInnerIndex() {
        return innerIndex;
    }

    public void setInnerIndex(int innerIndex) {
        this.innerIndex = innerIndex;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }
}
