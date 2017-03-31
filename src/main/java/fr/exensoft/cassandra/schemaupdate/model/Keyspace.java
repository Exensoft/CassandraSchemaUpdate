package fr.exensoft.cassandra.schemaupdate.model;

import java.util.ArrayList;
import java.util.List;

public class Keyspace {

    private String name;

    private List<Table> tables;

    public Keyspace(String name) {
        this.name = name;
        this.tables = new ArrayList<>();
    }


    public String getName() {
        return name;
    }

    public List<Table> getTables() {
        return tables;
    }

    public Table getTable(String table) {
        return tables.stream()
                .filter(t->t.getName().equals(table))
                .findFirst()
                .orElse(null);
    }

    public Keyspace addTable(Table table) {
        tables.add(table);
        table.setKeyspace(this);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("KeySpace ").append(name).append(" :");

        for(Table table : tables) {
            result.append("\n").append(table.toString());
        }

        return result.toString();
    }

    public void validate() {
        tables.forEach(Table::validate);
    }
}
