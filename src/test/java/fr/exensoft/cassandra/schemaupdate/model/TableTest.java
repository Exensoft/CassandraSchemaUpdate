package fr.exensoft.cassandra.schemaupdate.model;


import fr.exensoft.cassandra.schemaupdate.model.type.BasicType;
import fr.exensoft.cassandra.schemaupdate.model.values.SortOrder;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TableTest {

    @Test
    public void tableTest() {
        Column column1 = new Column("column1", BasicType.TEXT);
        Column column2 = new Column("column2", BasicType.TEXT);
        Column column3 = new Column("column3", BasicType.TEXT);
        Column column4 = new Column("column4", BasicType.TEXT);
        Column column5 = new Column("column5", BasicType.TEXT);

        Table table = new Table("test_table")
                .addColumn(column1)
                .addColumn(column2)
                .addColumn(column3)
                .addColumn(column4)
                .addColumn(column5)
                .addPartitioningKey("column1")
                .addClusteringColumn("column2")
                .addClusteringColumn("column3", SortOrder.DESC)
                .addIndex("test_table_column4_index", "column4");

        table.validate();

        assertThat(table.getColumn("column1")).isEqualTo(column1);
        assertThat(table.getColumn("column2")).isEqualTo(column2);
        assertThat(table.getColumn("column3")).isEqualTo(column3);
        assertThat(table.getColumn("column4")).isEqualTo(column4);
        assertThat(table.getColumn("column5")).isEqualTo(column5);

        assertThat(table.getClusteringColumns()).containsOnly(column2, column3);
        assertThat(table.getSortOrders().get(column2)).isEqualTo(SortOrder.ASC);
        assertThat(table.getSortOrders().get(column3)).isEqualTo(SortOrder.DESC);

        assertThat(table.getPartitioningKeys()).containsOnly(column1);

        assertThat(table.getIndex(column4)).isNotNull();
        assertThat(table.getIndex(column4).getName()).isEqualTo("test_table_column4_index");
    }

}
