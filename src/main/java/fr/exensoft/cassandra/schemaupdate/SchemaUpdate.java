package fr.exensoft.cassandra.schemaupdate;

import com.datastax.driver.core.Cluster;
import fr.exensoft.cassandra.schemaupdate.comparator.KeyspaceComparator;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaList;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.DeltaResult;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * SchemaUpdate is the main element of CassandraSchemaUpdate.
 * You can create patches and apply them to the Cassandra database
 */
public class SchemaUpdate {

    private final static Logger LOGGER = LoggerFactory.getLogger(SchemaUpdate.class);

    private CassandraConnection cassandraConnection;

    private SchemaUpdate(Builder builder) {
        cassandraConnection = builder.cassandraConnection;
        cassandraConnection.connect();
    }


    /**
     * Create a patch that describe the operations you need to execute to obtain the
     * targetKeyspace in your Cassandra database.
     * It loads the keyspace from the database if it exists and finds the differences between the
     * existing keyspace and the targetKeyspace
     *
     * @param targetKeyspace The target keyspace
     * @return The patch you need to execute to obtain the target keyspace
     */
    public DeltaResult createPatch(Keyspace targetKeyspace) {
        // Load current keyspace
        Keyspace sourceKeyspace = cassandraConnection.loadKeyspace(targetKeyspace.getName());

        // Comparing sourceKeyspace with targetKeyspace
        return new KeyspaceComparator(sourceKeyspace, targetKeyspace).compare();
    }

    /**
     * Apply a deltaList on database
     * @param deltaList
     */
    private void applyDeltaList(DeltaList deltaList) {
        for(AbstractDelta delta : deltaList.getDeltas()) {
            LOGGER.debug(delta.toString());
            cassandraConnection.applyDelta(delta);
        }
    }

    /**
     * Apply a patch to the database
     * @param deltaResult Patch to execute
     */
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

    /**
     * Close the CassandraConnection
     */
    public void close() {
        cassandraConnection.close();
    }

    /**
     * SchemaUpdate Builder
     * You can use a Cluster object or a CassandraConnection object directly
     */
    public static class Builder {
        private Cluster cluster;

        private CassandraConnection cassandraConnection;

        /**
         * A cluster element (not connected)
         * @param cluster
         * @return
         */
        public Builder withCluster(Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        /**
         * A CassandraConnection element
         * @param cassandraConnection
         * @return
         */
        public Builder withCassandraConnection(CassandraConnection cassandraConnection) {
            this.cassandraConnection = cassandraConnection;
            return this;
        }

        /**
         * Create an instance of SchemaUpdate with the described parameters
         * You must have defined a Cluster object or a CassandraConnection object.
         * If Cluster and CassandraConnection are both defined, CassandraConnection will be used.
         *
         * @return The created instance of SchemaUpdate
         */
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
