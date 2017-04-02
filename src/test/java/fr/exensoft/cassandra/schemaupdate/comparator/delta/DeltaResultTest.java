package fr.exensoft.cassandra.schemaupdate.comparator.delta;


import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.CreateColumnDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.CreateIndexDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.DropColumnDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.DropIndexDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.ElementType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.keyspace.CreateKeyspaceDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.CreateTableDelta;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class DeltaResultTest {

    @Test
    public void hasFlagTest() {
        DeltaList keyspaceList = new DeltaList();
        DeltaList table1List = new DeltaList();
        DeltaList table2List = new DeltaList();

        Map<String, DeltaList> tables = new HashMap<>();
        tables.put("table1", table1List);
        tables.put("table2", table2List);

        DeltaResult deltaResult = new DeltaResult("test", keyspaceList, tables);

        assertThat(deltaResult.hasFlag(DeltaFlag.DATA_LOSS)).isFalse();

        // Add a flag
        keyspaceList.addFlag(DeltaFlag.DATA_LOSS);
        assertThat(deltaResult.hasFlag(DeltaFlag.DATA_LOSS)).isTrue();

        keyspaceList.clear();

        table1List.addFlag(DeltaFlag.DATA_LOSS);
        assertThat(deltaResult.hasFlag(DeltaFlag.DATA_LOSS)).isTrue();

        table1List.clear();

        table2List.addFlag(DeltaFlag.DATA_LOSS);
        assertThat(deltaResult.hasFlag(DeltaFlag.DATA_LOSS)).isTrue();
    }

    @Test
    public void hasUpdateTest() {
        DeltaList keyspaceList = new DeltaList();
        DeltaList table1List = new DeltaList();
        DeltaList table2List = new DeltaList();

        Map<String, DeltaList> tables = new HashMap<>();
        tables.put("table1", table1List);
        tables.put("table2", table2List);

        DeltaResult deltaResult = new DeltaResult("test", keyspaceList, tables);

        assertThat(deltaResult.hasUpdate()).isFalse();

        // Add a delta
        keyspaceList.addDelta(new CreateKeyspaceDelta(new Keyspace("target")));
        assertThat(deltaResult.hasUpdate()).isTrue();

        keyspaceList.clear();

        table1List.addDelta(new CreateTableDelta(new Keyspace("target"), new Table("test")));
        assertThat(deltaResult.hasUpdate()).isTrue();

        table1List.clear();

        table2List.addDelta(new CreateTableDelta(new Keyspace("target"), new Table("test")));
        assertThat(deltaResult.hasUpdate()).isTrue();
    }

    @Test
    public void gettersTest() {
        DeltaList keyspaceList = new DeltaList();
        DeltaList table1List = new DeltaList();
        DeltaList table2List = new DeltaList();

        Map<String, DeltaList> tables = new HashMap<>();
        tables.put("table1", table1List);
        tables.put("table2", table2List);

        DeltaResult deltaResult = new DeltaResult("test", keyspaceList, tables);

        assertThat(deltaResult.getKeyspaceDelta()).isEqualTo(keyspaceList);
        assertThat(deltaResult.getTablesDelta()).isEqualTo(tables);
        assertThat(deltaResult.getKeyspace()).isEqualTo("test");
    }
}
