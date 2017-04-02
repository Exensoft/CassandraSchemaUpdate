package fr.exensoft.cassandra.schemaupdate.comparator.delta;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;

import java.util.Map;

public class DeltaResult {

    private String keyspace;

    private DeltaList keyspaceDelta;

    private Map<String, DeltaList> tablesDelta;

    public DeltaResult(String keyspace, DeltaList keyspaceDelta, Map<String, DeltaList> tablesDelta) {
        this.keyspace = keyspace;
        this.keyspaceDelta = keyspaceDelta;
        this.tablesDelta = tablesDelta;
    }

    /**
     * Returns true if at least one of the elements of the keyspace need update operations
     * @return
     */
    public boolean hasUpdate() {
        return keyspaceDelta.hasUpdate() || tablesDelta.values().stream().anyMatch(DeltaList::hasUpdate);
    }

    /**
     * Returns true if at least on of the elements of the keyspace contains the flag
     * @param flag
     * @return
     */
    public boolean hasFlag(DeltaFlag flag) {
        return keyspaceDelta.hasFlag(flag) || tablesDelta.values().stream().anyMatch(d->d.hasFlag(flag));
    }

    /**
     * Name of the keyspace
     * @return
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Returns the deltaList of the keyspace's structure
     * @return
     */
    public DeltaList getKeyspaceDelta() {
        return keyspaceDelta;
    }

    /**
     * Returns a map containing the deltaList of each tables of the keyspace
     * @return
     */
    public Map<String, DeltaList> getTablesDelta() {
        return tablesDelta;
    }
}
