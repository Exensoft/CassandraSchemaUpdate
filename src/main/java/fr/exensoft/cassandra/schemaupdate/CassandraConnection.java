package fr.exensoft.cassandra.schemaupdate;

import com.datastax.driver.core.*;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.loader.ClusterSchemaLoader;
import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;
import fr.exensoft.cassandra.schemaupdate.loader.SchemaLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraConnection implements SchemaLoader {

	private final static Logger LOGGER = LoggerFactory.getLogger(CassandraConnection.class);

	private Session session = null;
	private Cluster cluster;

	private boolean isConnected = false;

	private SchemaLoader schemaLoader = null;

	CassandraConnection(Cluster cluster) {
		this.cluster = cluster;
	}

	public void connect() {
		if(isConnected) {
			return;
		}
		isConnected = true;

		session = cluster.connect();

	}

	public void close() {
		if(!isConnected) {
			return;
		}
		isConnected = false;


		session.close();
		cluster.close();
	}

	public void applyDelta(AbstractDelta delta) {
		session.execute(delta.generateCQL());
	}


    @Override
    public List<String> listKeyspaces() {
	    return getSchemaLoader().listKeyspaces();
    }

    @Override
    public Keyspace loadKeyspace(String name) {
        return getSchemaLoader().loadKeyspace(name);
    }

    @Override
    public List<Table> loadTables(String keyspace_name) {
        return getSchemaLoader().loadTables(keyspace_name);
    }

    @Override
    public Table loadTable(String keyspace_name, String table_name) {
        return getSchemaLoader().loadTable(keyspace_name, table_name);
    }

    private SchemaLoader getSchemaLoader() {
	    if(schemaLoader != null) {
	        return schemaLoader;
        }

        schemaLoader = new ClusterSchemaLoader(cluster);
        return schemaLoader;
    }
}