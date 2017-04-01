package fr.exensoft.cassandra.schemaupdate.comparator;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaResult;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace.CreateKeyspaceDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace.DropKeyspaceDelta;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * Find differences between two keyspaces by returning a delta list.
 */
public class KeyspaceComparator {

    private Keyspace source;
    private Keyspace target;

    private DeltaList keyspaceDelta;

    private Map<String, DeltaList> tablesDelta;

    /**
     * Create a new Keyspace comparator.
     *
     * @param source
     * @param target
     */
    public KeyspaceComparator(Keyspace source, Keyspace target) {
        this.source = source;
        this.target = target;
    }

    /**
     * Find differences between source keyspace and target keyspace
     * (only keyspace differences, not differences in tables of keyspaces)
     */
    private void compareKeyspaces() {
        keyspaceDelta = new DeltaList();

        //Detect keyspace creation
        if(source == null && target != null) {
            keyspaceDelta.addDelta(new CreateKeyspaceDelta(target));
        }

        //Detect keyspace deletion
        if(source != null && target == null) {
            keyspaceDelta.addDelta(new DropKeyspaceDelta(source));
            keyspaceDelta.addFlag(DeltaFlag.DATA_LOSS);
        }

        //Detect keyspace name change
        if(source != null && target != null && !source.getName().equals(target.getName())) {
            keyspaceDelta.addDelta(new DropKeyspaceDelta(source));
            keyspaceDelta.addDelta(new CreateKeyspaceDelta(target));
            keyspaceDelta.addFlag(DeltaFlag.DATA_LOSS);
        }

    }

    /**
     * Find differences between source table and target table by using a TableComparator
     * @param source
     * @param target
     */
    private void compareTables(Table source, Table target) {
        TableComparator tableComparator = new TableComparator(source, target);
        tablesDelta.put(tableComparator.getTableName(), tableComparator.compare());
    }

    /**
     * Find differences between source keyspace and target keyspace and their tables
     *
     * @return A DeltaResult instance that will describe differences for each structure of keyspaces
     */
    public DeltaResult compare() {
        tablesDelta = new HashMap<>();

        // Detect keyspace changes
        compareKeyspaces();

        // Check tables changes
        if(target != null) {
            // If source keyspace is suppressed we just need to create tables
            if(keyspaceDelta.hasFlag(DeltaFlag.DATA_LOSS) || source == null) {
                for(Table table : target.getTables()) {
                    compareTables(null, table);
                }
            }
            else {
                for(Table targetTable : target.getTables()) {
                    Table sourceTable = source.getTable(targetTable.getName());
                    compareTables(sourceTable, targetTable);
                }
                for(Table sourceTable : source.getTables()) {
                    Table targetTable = target.getTable(sourceTable.getName());
                    if(targetTable == null) {
                        compareTables(sourceTable, null);
                    }
                }
            }
        }

        String name = (target!=null)?target.getName():source.getName();

        return new DeltaResult(name, keyspaceDelta, tablesDelta);
    }

}
