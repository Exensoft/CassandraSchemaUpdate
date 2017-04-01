package fr.exensoft.cassandra.schemaupdate.comparator;


import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaResult;
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

public class KeyspaceComparatorTest {

    @Test
    public void createKeyspaceTest() {

        Keyspace targetKeyspace = new Keyspace("test")
                .addTable(new Table("table1")
                        .addColumn(new Column("column1", BasicType.UUID))
                        .addColumn(new Column("column2", BasicType.VARCHAR))
                        .addColumn(new Column("column3", BasicType.VARINT))
                        .addPartitioningKey("column1")
                )
                .addTable(new Table("table2")
                        .addColumn(new Column("column1", BasicType.TEXT))
                        .addPartitioningKey("column1")
                )
                ;


        KeyspaceComparator keyspaceComparator = new KeyspaceComparator(null, targetKeyspace);

        DeltaResult result = keyspaceComparator.compare();

        assertThat(result.hasUpdate()).isTrue();

    }

}
