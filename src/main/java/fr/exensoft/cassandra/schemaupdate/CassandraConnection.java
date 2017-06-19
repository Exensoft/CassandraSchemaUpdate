package fr.exensoft.cassandra.schemaupdate;

import com.datastax.driver.core.*;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.AbstractDelta;
import fr.exensoft.cassandra.schemaupdate.loader.CassandraV2SchemaLoader;
import fr.exensoft.cassandra.schemaupdate.loader.CassandraV3SchemaLoader;
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
	    List<KeyspaceMetadata> tests = cluster.getMetadata().getKeyspaces();



        //return getSchemaLoader().listKeyspaces();
        return null;
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
        /*
        int majorVersion = getMajorVersion();
        if(majorVersion == 2) {
            schemaLoader = new CassandraV2SchemaLoader(session);
            return schemaLoader;
        }
        else if(majorVersion == 3) {
            schemaLoader = new CassandraV3SchemaLoader(session);
            return schemaLoader;
        }
        else {
	        throw new SchemaUpdateException(String.format("Unhandled major version %d", majorVersion));
        }*/
        schemaLoader = new ClusterSchemaLoader(cluster);
        return schemaLoader;
    }

    private int getMajorVersion() {
        ResultSet rs = session.execute("select release_version from system.local");
        Row row = rs.one();
        if(row == null) {
            return 0;
        }
        String version = row.getString("release_version");
        if(version.charAt(0) == '2') {
            return 2;
        }
        else if(version.charAt(0) == '3') {
            return 3;
        }
        return 0;
    }
}