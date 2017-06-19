package fr.exensoft.cassandra.schemaupdate.cluster;


import com.datastax.driver.core.*;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class CassandraTestUtils {

    protected ResultSet createResultSet(List<Row> rows) {
        ResultSet rs = Mockito.mock(ResultSet.class);
        if(rows.size() > 0) {
            Mockito.doReturn(rows.get(0)).when(rs).one();
        }
        else {
            Mockito.doReturn(null).when(rs).one();
        }

        Mockito.doReturn(rows.iterator()).when(rs).iterator();
        return rs;
    }

    protected Row createVersionRow(String version) {
        Row row = Mockito.mock(Row.class);
        Mockito.doReturn(version).when(row).getString(Mockito.eq("release_version"));
        return row;
    }

}
