package fr.exensoft.cassandra.schemaupdate.comparator.delta;

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

    public boolean hasUpdate() {
        return keyspaceDelta.hasUpdate() || tablesDelta.values().stream().anyMatch(DeltaList::hasUpdate);
    }

    public String getKeyspace() {
        return keyspace;
    }

    public DeltaList getKeyspaceDelta() {
        return keyspaceDelta;
    }

    public Map<String, DeltaList> getTablesDelta() {
        return tablesDelta;
    }
}
