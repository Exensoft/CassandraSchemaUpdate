package fr.exensoft.cassandra.schemaupdate;

import com.datastax.driver.core.PoolingOptions;
import fr.exensoft.cassandra.schemaupdate.comparator.SchemaComparator;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaResult;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SchemaUpdate {

    private final static Logger LOGGER = LoggerFactory.getLogger(SchemaUpdate.class);

    private CassandraConnection cassandraConnection;

    private SchemaUpdate(Builder builder) {
        this.cassandraConnection = new CassandraConnection(builder.contactPoints, builder.port, builder.poolingOptions);
        this.cassandraConnection.connect();
    }


    public DeltaResult createPatch(Keyspace targetKeyspace) {
        // Load current keyspace
        Keyspace sourceKeyspace = cassandraConnection.loadKeyspace(targetKeyspace.getName());

        // Comparing sourceKeyspace with targetKeyspace
        return new SchemaComparator(sourceKeyspace, targetKeyspace).compare();
    }

    private void applyDeltaList(DeltaList deltaList) {
        for(AbstractDelta delta : deltaList.getDeltas()) {
            LOGGER.debug(delta.toString());
            cassandraConnection.applyDelta(delta);
        }
    }

    public void applyPatch(DeltaResult deltaResult) {
        if(!deltaResult.hasUpdate()) {
            LOGGER.info("No update for keyspace {}", deltaResult.getKeyspace());
            return;
        }

        if(deltaResult.getKeyspaceDelta().hasUpdate()) {
            LOGGER.info("Applying patch on keyspace {}", deltaResult.getKeyspace());
            applyDeltaList(deltaResult.getKeyspaceDelta());
        }

        for(Map.Entry<String, DeltaList> entry : deltaResult.getTablesDelta().entrySet()) {
            if(entry.getValue().hasUpdate()) {
                LOGGER.info("Applying patch on table {}", entry.getKey());
                applyDeltaList(entry.getValue());
            }
        }

    }

    public static class Builder {
        private List<String> contactPoints = new LinkedList<>();
        private PoolingOptions poolingOptions;
        private int port = 9042;

        public Builder addContactPoint(String contactPoint) {
            contactPoints.add(contactPoint);
            return this;
        }

        public Builder withPoolingOptions(PoolingOptions poolingOptions) {
            this.poolingOptions = poolingOptions;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public SchemaUpdate build() {
            return new SchemaUpdate(this);
        }
    }
}
