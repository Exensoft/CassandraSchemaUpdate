package fr.exensoft.cassandra.schemaupdate.comparator;


import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.*;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.CreateTableDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.DropTableDelta;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Index;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import fr.exensoft.cassandra.schemaupdate.utils.CQLTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Find differences between two tables by returning a delta list.
 */
public class TableComparator {

    private final static Logger LOGGER = LoggerFactory.getLogger(TableComparator.class);

    private static enum CType { PARTITIONING_KEY, CLUSTERING_KEY, COLUMN }

    //Source table
    private Table source;
    //Target table
    private Table target;

    // Default keyspace (target keyspace)
    private Keyspace keyspace;

    /**
     * Create a TableComparator that will find differences between source table and target table.
     * If source table is null, target table will be created.
     * If target table is null, source table will be dropped.
     *
     * You can not use a null source table and a null target table
     *
     * @param source Source table
     * @param target Target table
     */
    public TableComparator(Table source, Table target) {
        this.source = source;
        this.target = target;
        this.keyspace = (target != null)?target.getKeyspace():source.getKeyspace();
    }

    /**
     * Returns the name of the target table if target table is not null.
     * Returns the name of the source table if target table is null.
     *
     * @return The name of the concerned table.
     */
    public String getTableName() {
        if(target != null) {
            return target.getName();
        }
        return source.getName();
    }

    /**
     * Find differences between two columns
     *
     * @param deltaList The DeltaList where differences will be added
     * @param sourceColumn The source column
     * @param targetColumn The target column
     */
    private void compareColumn(DeltaList deltaList, Column sourceColumn, Column targetColumn) {

        //Check if column has been renamed
        if(!sourceColumn.getName().equals(targetColumn.getName())) {
            deltaList.addDelta(new RenameColumnDelta(keyspace, target, sourceColumn, targetColumn));
        }

        if(!CQLTypeUtils.equals(sourceColumn.getType(), targetColumn.getType())) {
            deltaList.addDelta(new AlterTypeColumnDelta(keyspace, target, sourceColumn, targetColumn));
        }

        CType sourceType = getCType(source, sourceColumn);
        CType targetType = getCType(target, targetColumn);
        List<ColumnAbstractDelta> columnDelta = getColumnDelta(deltaList, targetColumn);

        //No need to check if keys are modified (need to recreate the table)
        if(sourceType != targetType) {
            return;
        }

        //No need to check if there is no delata
        if(columnDelta.isEmpty()) {
            return;
        }

        // Rules :
        // Renaming column only for clustering keys or partitioning keys
        // Alter type :
        //  - Apply order type compatibility for indexes and for clustering keys
        //  - Apply type compatibility for partitioning keys and normal columns

        if(sourceType == CType.PARTITIONING_KEY) {
            if(hasDelta(columnDelta, DeltaType.ALTER_TYPE)) {
                if(!CQLTypeUtils.isCompatible(sourceColumn.getType(), targetColumn.getType())) {
                    //Incompatible types
                    LOGGER.debug("Table {}, column {}, incompatible types : {} --> {}", target.getName(), targetColumn.getName(), sourceColumn.getType().getType(), targetColumn.getType().getType());
                    deltaList.addFlag(DeltaFlag.NEED_RECREATE);
                }
            }
        }
        else if(targetType == CType.CLUSTERING_KEY) {
            if(hasDelta(columnDelta, DeltaType.ALTER_TYPE)) {
                if(!CQLTypeUtils.isOrderCompatible(sourceColumn.getType(), targetColumn.getType())) {
                    //Incompatible types
                    LOGGER.debug("Table {}, column {}, order incompatible types : {} --> {}", target.getName(), targetColumn.getName(), sourceColumn.getType().getType(), targetColumn.getType().getType());
                    deltaList.addFlag(DeltaFlag.NEED_RECREATE);
                }
            }
        }
        else {
            if(hasDelta(columnDelta, DeltaType.ALTER_TYPE)) {
                boolean tIndex = (target.getIndex(targetColumn) != null);
                boolean sIndex = (source.getIndex(sourceColumn) != null);
                if(tIndex && sIndex) {
                    //If column is an index column, types need to be order compatible
                    if(!CQLTypeUtils.isOrderCompatible(sourceColumn.getType(), targetColumn.getType())) {
                        //If types are compatible but not order compatible, we can recreate the index only (not the whole column)
                        if (!CQLTypeUtils.isCompatible(sourceColumn.getType(), targetColumn.getType())) {
                            //Incompatible types
                            LOGGER.debug("Table {}, column {}, incompatible types : {} --> {}", target.getName(), targetColumn.getName(), sourceColumn.getType().getType(), targetColumn.getType().getType());
                            clearColumnDelta(deltaList, targetColumn);
                            deltaList.addDelta(new DropColumnDelta(keyspace, source, sourceColumn));
                            deltaList.addDelta(new CreateColumnDelta(keyspace, target, targetColumn));
                            deltaList.addFlag(DeltaFlag.ORDER_CHANGED);
                            deltaList.addFlag(DeltaFlag.DATA_LOSS);
                        }

                        if(!hasDelta(columnDelta, DeltaType.CREATE_INDEX)) {
                            deltaList.addDelta(new DropIndexDelta(keyspace, source, sourceColumn, targetColumn));
                            deltaList.addDelta(new CreateIndexDelta(keyspace, target, sourceColumn, targetColumn));
                        }

                    }
                }
                else {
                    if (!CQLTypeUtils.isCompatible(sourceColumn.getType(), targetColumn.getType())) {
                        //Incompatible types
                        LOGGER.debug("Table {}, column {}, incompatible types : {} --> {}", target.getName(), targetColumn.getName(), sourceColumn.getType().getType(), targetColumn.getType().getType());
                        clearColumnDelta(deltaList, targetColumn);
                        deltaList.addDelta(new DropColumnDelta(keyspace, source, sourceColumn));
                        deltaList.addDelta(new CreateColumnDelta(keyspace, target, targetColumn));
                        deltaList.addFlag(DeltaFlag.ORDER_CHANGED);
                        deltaList.addFlag(DeltaFlag.DATA_LOSS);
                    }
                }
            }

            //Renaming a non key column is not allowed
            if(hasDelta(columnDelta, DeltaType.RENAME)) {
                LOGGER.debug("Table {}, column {}, can not rename non key columns : {} --> {}", target.getName(), targetColumn.getName(), sourceColumn.getName(), targetColumn.getName());
                clearColumnDelta(deltaList, targetColumn);
                deltaList.addDelta(new DropColumnDelta(keyspace, source, sourceColumn));
                deltaList.addDelta(new CreateColumnDelta(keyspace, target, targetColumn));
                deltaList.addFlag(DeltaFlag.ORDER_CHANGED);
                deltaList.addFlag(DeltaFlag.DATA_LOSS);
            }
        }

    }


    /**
     * Find indexes differences
     * @param deltaList The DeltaList where differences will be added
     */
    private void compareIndexes(DeltaList deltaList) {

        //Find deleted (and modified) indexes (present in source table but not in target table)
        for(Column sourceColumn : source.getColumns()) {
            Index sourceIndex = source.getIndex(sourceColumn);
            Index targetIndex = null;

            Column targetColumn = target.getColumn(sourceColumn.getName());
            if(targetColumn != null) {
                targetIndex = target.getIndex(targetColumn);
            }

            if(sourceIndex == null) {
                continue;
            }

            if(targetIndex == null) {
                deltaList.addDelta(new DropIndexDelta(keyspace, source, sourceColumn, targetColumn));
            }
            else if(!targetIndex.getName().equals(sourceIndex.getName())) {
                //If an index is modified we need to recreate it
                deltaList.addDelta(new DropIndexDelta(keyspace, source, sourceColumn, targetColumn));
                deltaList.addDelta(new CreateIndexDelta(keyspace, target, sourceColumn, targetColumn));
            }
        }

        //Find added indexes (present in target table but not in source table)
        for(Column targetColumn : target.getColumns()) {
            Index targetIndex = target.getIndex(targetColumn);

            Index sourceIndex = null;

            Column sourceColumn = source.getColumn(targetColumn.getName());
            if(sourceColumn != null) {
                sourceIndex = source.getIndex(sourceColumn);
            }

            if(targetIndex == null) {
                continue;
            }

            if(sourceIndex == null) {
                deltaList.addDelta(new CreateIndexDelta(keyspace, target, sourceColumn, targetColumn));
            }
        }
    }

    /**
     * Find differences between columns
     * @param deltaList The DeltaList where differences will be added
     */
    private void compareColumns(DeltaList deltaList) {
        List<String> deletedColumns = new ArrayList<>();
        List<String> createdColumns = new ArrayList<>();
        Map<String, String> renamedColumns = new HashMap<String, String>();

        //Find deleted columns (present in source table but not in target table)
        for(Column sourceColumn : source.getColumns()) {
            Column targetColumn = target.getColumn(sourceColumn.getName());
            if(targetColumn == null) {
                deletedColumns.add(sourceColumn.getName());
            }
        }

        //Find added columns (present in target table but not in source table)
        for(Column targetColumn : target.getColumns()) {
            Column sourceColumn = source.getColumn(targetColumn.getName());
            if(sourceColumn == null) {

                //Check if column has been renamed
                Optional<String> oldName = targetColumn.getOldNames().stream()
                        .filter(deletedColumns::contains)
                        .findFirst();

                if(oldName.isPresent()) {
                    deletedColumns.remove(oldName.get());
                    renamedColumns.put(oldName.get(), targetColumn.getName());
                }
                else {
                    createdColumns.add(targetColumn.getName());
                }
            }
        }

        //Find order modification on existing columns
        boolean isSameOrder = true;
        boolean hasCreatedColumn = false;
        int s_i = 0, t_i = 0;
        while(s_i < source.getColumns().size() && t_i < target.getColumns().size()) {
            Column sourceColumn = source.getColumns().get(s_i);
            Column targetColumn = target.getColumns().get(t_i);

            //If source column is a deleted column
            if(deletedColumns.contains(sourceColumn.getName())) {
                s_i++;
                continue;
            }

            //If target column is a new column
            if(createdColumns.contains(targetColumn.getName())) {
                hasCreatedColumn = true;
                t_i++;
                continue;
            }
            else {
                //If created columns are not at the end of the table, they change the order of columns
                if(hasCreatedColumn) {
                    isSameOrder = false;
                }
            }

            String sourceName = sourceColumn.getName();

            //Get new name if source column has been renamed
            if(renamedColumns.containsKey(sourceName)) {
                sourceName = renamedColumns.get(sourceName);
            }

            if(!sourceName.equals(targetColumn.getName())) {
                isSameOrder = false;
                targetColumn = target.getColumn(sourceName);
            }

            compareColumn(deltaList, sourceColumn, targetColumn);

            s_i++;
            t_i++;
        }

        //Add modifications to deltaList
        deletedColumns.stream()
                .map(source::getColumn)
                .map(column -> new DropColumnDelta(keyspace, source, column))
                .forEach(deltaList::addDelta);
        createdColumns.stream()
                .map(target::getColumn)
                .map(column -> new CreateColumnDelta(keyspace, target, column))
                .forEach(deltaList::addDelta);

        if(!deletedColumns.isEmpty()) {
            deltaList.addFlag(DeltaFlag.DATA_LOSS);
        }

        if(!isSameOrder) {
            deltaList.addFlag(DeltaFlag.ORDER_CHANGED);
        }

        //Find key modification
        if(!compareKeys(source.getPartitioningKeys(), target.getPartitioningKeys(), renamedColumns)) {
            LOGGER.debug("Partitionning key changed : {} --> {}", source.getPartitioningKeys(), target.getPartitioningKeys());
            deltaList.addFlag(DeltaFlag.NEED_RECREATE);
        }

        if(!compareKeys(source.getClusteringColumns(), target.getClusteringColumns(), renamedColumns)) {
            LOGGER.debug("Clustering key changed");
            deltaList.addFlag(DeltaFlag.NEED_RECREATE);
        }
        else {
            Map<String, SortOrder> sourceOrder = new HashMap<>();
            for(Map.Entry<Column, SortOrder> e : source.getSortOrders().entrySet()) {
                if(renamedColumns.containsKey(e.getKey().getName())) {
                    sourceOrder.put(renamedColumns.get(e.getKey().getName()), e.getValue());
                }
                else {
                    sourceOrder.put(e.getKey().getName(), e.getValue());
                }
            }
            Map<String, SortOrder> targetOrder = new HashMap<>();
            for(Map.Entry<Column, SortOrder> e : target.getSortOrders().entrySet()) {
                targetOrder.put(e.getKey().getName(), e.getValue());
            }
            if(!sourceOrder.equals(targetOrder)) {
                LOGGER.debug("Clustering key sorting order changed");
                deltaList.addFlag(DeltaFlag.NEED_RECREATE);
            }
        }
    }

    /**
     * Compare keys, return true if keys are the same
     * @param sourceKey
     * @param targetKey
     * @param renamedColumns
     * @return
     */
    private boolean compareKeys(List<Column> sourceKey, List<Column> targetKey, Map<String, String> renamedColumns) {
        if(sourceKey.size() != targetKey.size()) {
            return false;
        }

        for(int i=0;i<sourceKey.size();i++) {
            String source = sourceKey.get(i).getName();
            String target = targetKey.get(i).getName();
            if(renamedColumns.containsKey(source)) {
                source = renamedColumns.get(source);
            }
            if(!source.equals(target)) {
                System.out.println("DIFF "+source+" <- "+target);
                return false;
            }
        }

        return true;
    }

    /**
     * Compare the source table with the target table and find the differences.
     * Differences will be returned as a DeltaList that contains a sequence of operations to
     * transform the source table into the target table
     *
     * @return A DeltaList object that describe the operations to apply
     */
    public DeltaList compare() {
        if(source != null) {
            source.validate();
        }
        if(target != null) {
            target.validate();
        }

        DeltaList delta = new DeltaList();

        if(source == null && target != null) {
            //New table
            createTable(delta);
        }
        else if(source != null && target == null) {
            //Delete table
            deleteTable(delta);
        }
        else {
            //Find delta
            compareIndexes(delta);
            compareColumns(delta);

            if(delta.hasFlag(DeltaFlag.NEED_RECREATE)) {
                delta.clear();
                delta.addFlag(DeltaFlag.NEED_RECREATE);
                deleteTable(delta);
                createTable(delta);
            }
        }

        delta.sort();

        return delta;
    }

    /**
     * Add the create table delta (CREATE TABLE and CREATE INDEX)
     * @param deltaList
     */
    private void createTable(DeltaList deltaList) {
        deltaList.addDelta(new CreateTableDelta(keyspace, target));
        for(Index index : target.getIndexes()) {
            deltaList.addDelta(new CreateIndexDelta(keyspace, target, null, index.getColumn()));
        }
    }

    /**
     * Add the drop table detla
     * @param deltaList
     */
    private void deleteTable(DeltaList deltaList) {
        deltaList.addFlag(DeltaFlag.DATA_LOSS);
        deltaList.addDelta(new DropTableDelta(keyspace, source));
        for(Index index : source.getIndexes()) {
            deltaList.addDelta(new DropIndexDelta(keyspace, source, index.getColumn(), null));
        }
    }

    private void clearColumnDelta(DeltaList deltaList, Column column) {
        deltaList.getDeltas().removeAll(getColumnDelta(deltaList, column));
    }

    private List<ColumnAbstractDelta> getColumnDelta(DeltaList deltaList, Column column) {
        return deltaList.getDeltas().stream()
                .filter(c->c instanceof ColumnAbstractDelta)
                .map(c->(ColumnAbstractDelta) c)
                .filter(c->(c.getTarget() == column || c.getSource() == column))
                .collect(Collectors.toList());
    }

    private boolean hasDelta(List<ColumnAbstractDelta> deltas, DeltaType... types) {
        return deltas.stream().anyMatch(delta->Arrays.stream(types).anyMatch(type->type.equals(delta.getDeltaType())));
    }

    private CType getCType(Table table, Column column) {
        if(table.getPartitioningKeys().contains(column)) {
            return CType.PARTITIONING_KEY;
        }
        else if(table.getClusteringColumns().contains(column)) {
            return CType.CLUSTERING_KEY;
        }
        else {
            return CType.COLUMN;
        }
    }
}
