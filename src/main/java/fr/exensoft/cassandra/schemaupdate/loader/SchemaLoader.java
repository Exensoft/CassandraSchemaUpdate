package fr.exensoft.cassandra.schemaupdate.loader;

import fr.exensoft.cassandra.schemaupdate.model.Keyspace;
import fr.exensoft.cassandra.schemaupdate.model.Table;

import java.util.List;

public interface SchemaLoader {

    /**
     * Returns a list of cluster's Keyspaces
     * @return
     */
    public List<String> listKeyspaces();

    /**
     * Returns a Keyspace
     * @param name
     * @return
     */
    public Keyspace loadKeyspace(String name);


    public List<Table> loadTables(String keyspace_name);

    public Table loadTable(String keyspace_name, String table_name);
}
