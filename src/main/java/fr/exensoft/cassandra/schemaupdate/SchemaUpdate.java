package fr.exensoft.cassandra.schemaupdate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import fr.exensoft.cassandra.schemaupdate.comparator.KeyspaceComparator;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaResult;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * SchemaUpdate  
 */
public class SchemaUpdate {

    private final static Logger LOGGER = LoggerFactory.getLogger(SchemaUpdate.class);

    private CassandraConnection cassandraConnection;

    private SchemaUpdate(Builder builder) {
        cassandraConnection = builder.cassandraConnection;
        cassandraConnection.connect();
    }


    public DeltaResult createPatch(Keyspace targetKeyspace) {
        // Load current keyspace
        Keyspace sourceKeyspace = cassandraConnection.loadKeyspace(targetKeyspace.getName());

        // Comparing sourceKeyspace with targetKeyspace
        return new KeyspaceComparator(sourceKeyspace, targetKeyspace).compare();
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
        private Cluster cluster;

        private CassandraConnection cassandraConnection;

        public Builder withCluster(Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        public Builder withCassandraConnection(CassandraConnection cluster) {
            this.cassandraConnection = cassandraConnection;
            return this;
        }

        public SchemaUpdate build() {
            if(cassandraConnection == null) {
                if(cluster == null) {
                    throw new SchemaUpdateException("You must define a Cluster or a CassandraConnection object");
                }
                cassandraConnection = new CassandraConnection(cluster);
            }
            return new SchemaUpdate(this);
        }
    }
}
