package fr.exensoft.cassandra.schemaupdate.model;

import fr.exensoft.cassandra.schemaupdate.SchemaUpdateException;
import fr.exensoft.cassandra.schemaupdate.model.values.IndexOption;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import sun.reflect.generics.tree.BaseType;

import java.util.*;

public class Table {

    private String name;

    private List<Column> columns;

    private List<Column> partitioningKeys;

    private List<Column> clusteringColumns;

    private Map<Column, SortOrder> sortOrders;

    private List<Index> indexes;

    private int innerIndex = 0;

    public Table(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.partitioningKeys = new ArrayList<>();
        this.clusteringColumns = new ArrayList<>();
        this.sortOrders = new HashMap<>();
        this.indexes = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Column> getPartitioningKeys() {
        return partitioningKeys;
    }
    public List<Column> getClusteringColumns() {
        return clusteringColumns;
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    public Table addColumn(Column column) {
        if(hasColumn(column.getName())) {
            throw new SchemaUpdateException(String.format("Table \"%s\" already has a column with name \"%s\"", name, column.getName()));
        }
        column.setInnerIndex(innerIndex);
        innerIndex++;
        columns.add(column);
        return this;
    }

    public Table addPartitioningKey(String columnName) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        column.setIndex(partitioningKeys.size());
        partitioningKeys.add(column);
        return this;
    }

    public Table addClusteringColumn(String columnName) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        column.setIndex(clusteringColumns.size());
        if(!sortOrders.containsKey(column)) {
            sortOrders.put(column, SortOrder.ASC);
        }
        clusteringColumns.add(column);
        return this;
    }

    public Table addClusteringColumn(String columnName, SortOrder sortOrder) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        column.setIndex(clusteringColumns.size());
        sortOrders.put(column, sortOrder);
        clusteringColumns.add(column);
        return this;
    }

    public Table addIndex(String name, String columnName) {
        return addIndex(name, columnName, IndexOption.VALUES);
    }

    public Table addIndex(String name, String columnName, IndexOption indexOption) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }
        Index index = new Index(name, column);

        if(!(column.getType() instanceof BaseType)) {
            index.addOption(indexOption);
        }

        indexes.add(index);
        return this;
    }

    public Table setOrder(String columnName, SortOrder sortOrder) {
        Column column = getColumn(columnName);
        if(column == null) {
            throw new SchemaUpdateException(String.format("Table \"%s\" has no column named \"%s\"", name, columnName));
        }

        sortOrders.put(column, sortOrder);

        return this;
    }

    public Map<Column, SortOrder> getSortOrders() {
        return sortOrders;
    }

    public void validate() {
        //Reorganize columns order
        Collections.sort(columns, (a,b)->{
            int priorityA = partitioningKeys.contains(a)?3:(clusteringColumns.contains(a)?2:1);
            int priorityB = partitioningKeys.contains(b)?3:(clusteringColumns.contains(b)?2:1);
            int comparation = -Integer.compare(priorityA, priorityB);
            if(comparation == 0) {
                if(priorityA > 1) {
                    return Integer.compare(a.getIndex(), b.getIndex());
                }
                return Integer.compare(a.getInnerIndex(), b.getInnerIndex());
            }
            return comparation;
        });
    }

    public Column getColumn(String name) {
        return columns.stream()
                .filter(c->c.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    public Index getIndex(Column column) {
        return indexes.stream()
                .filter(i->i.getColumn() == column)
                .findFirst()
                .orElse(null);
    }


    private boolean hasColumn(String name) {
        return columns.stream()
                .anyMatch(c->c.getName().equals(name));
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\tColumns :\n");
        for(Column column : columns) {
            sb.append("\t\t - ").append(column.getName()).append(" ").append(column.getType()).append("\n");
        }
        sb.append("\tPartitioning keys : \n");
        for(Column column : partitioningKeys) {
            sb.append("\t\t - ").append(column.getName()).append("\n");
        }
        sb.append("\tClustering keys : \n");
        for(Column column : clusteringColumns) {
            sb.append("\t\t - ").append(column.getName()).append(" (").append(sortOrders.get(column)).append(")\n");
        }

        return sb.toString();
    }
}
