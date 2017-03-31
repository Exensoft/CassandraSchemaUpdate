package fr.exensoft.cassandra.schemaupdate.comparator.delta;


import fr.exensoft.cassandra.schemaupdate.comparator.TableComparator;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.columns.*;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.CreateTableDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.table.DropTableDelta;
import fr.exensoft.cassandra.schemaupdate.model.Column;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class DeltaListTest {

    @Test
    public void addFlagTest() {

        DeltaList deltaList = new DeltaList();

        // Flag list must be empty
        assertThat(deltaList.getFlags()).isEmpty();

        // Add a flag
        deltaList.addFlag(DeltaFlag.DATA_LOSS);

        // Check flags
        assertThat(deltaList.getFlags()).contains(DeltaFlag.DATA_LOSS);
    }

    @Test
    public void hasFlagTest() {
        DeltaList deltaList = new DeltaList();

        // Add a flag
        deltaList.addFlag(DeltaFlag.DATA_LOSS);
        deltaList.addFlag(DeltaFlag.NEED_RECREATE);

        // Check flags
        assertThat(deltaList.getFlags()).containsOnly(DeltaFlag.DATA_LOSS, DeltaFlag.NEED_RECREATE);
    }

    @Test
    public void clearTest() {
        DeltaList deltaList = new DeltaList();

        // Add a flag
        deltaList.addFlag(DeltaFlag.DATA_LOSS);

        // Add a delta
        CreateTableDelta createTableDelta = new CreateTableDelta(new Keyspace("test"), new Table("table"));
        deltaList.addDelta(createTableDelta);

        // Check flags and delta
        assertThat(deltaList.hasFlag(DeltaFlag.DATA_LOSS)).isTrue();
        assertThat(deltaList.getDeltas()).containsOnly(createTableDelta);

        // Clear deltaList
        deltaList.clear();


        // Check flags and delta
        assertThat(deltaList.hasFlag(DeltaFlag.DATA_LOSS)).isFalse();
        assertThat(deltaList.getDeltas()).isEmpty();
        assertThat(deltaList.getFlags()).isEmpty();
    }

    @Test
    public void hasUpdateTest() {
        DeltaList deltaList = new DeltaList();

        assertThat(deltaList.hasUpdate()).isFalse();

        // Add a flag
        deltaList.addFlag(DeltaFlag.DATA_LOSS);

        assertThat(deltaList.hasUpdate()).isFalse();

        // Add a delta
        CreateTableDelta createTableDelta = new CreateTableDelta(new Keyspace("test"), new Table("table"));
        deltaList.addDelta(createTableDelta);

        assertThat(deltaList.hasUpdate()).isTrue();

        // Clear deltaList
        deltaList.clear();

        assertThat(deltaList.hasUpdate()).isFalse();
    }

    @Test
    public void sortTest() {
        DeltaList deltaList = new DeltaList();

        // Add a delta
        CreateColumnDelta createColumn = new CreateColumnDelta(new Keyspace("test"), new Table("table"), new Column("test", BasicType.VARINT));
        DropColumnDelta dropColumn = new DropColumnDelta(new Keyspace("test"), new Table("table"), new Column("test", BasicType.VARINT));
        DropIndexDelta dropIndex = new DropIndexDelta(new Keyspace("test"), new Table("table"), new Column("test", BasicType.VARINT), new Column("test", BasicType.VARINT));
        CreateIndexDelta createIndex = new CreateIndexDelta(new Keyspace("test"), new Table("table"), new Column("test", BasicType.VARINT), new Column("test", BasicType.VARINT));

        deltaList.addDelta(createColumn);
        deltaList.addDelta(dropColumn);
        deltaList.addDelta(dropIndex);
        deltaList.addDelta(createIndex);

        assertThat(deltaList.getDeltas()).containsExactly(createColumn, dropColumn, dropIndex, createIndex);

        deltaList.sort();
        assertThat(deltaList.getDeltas()).containsExactly(dropIndex, dropColumn, createColumn, createIndex);

    }
}
