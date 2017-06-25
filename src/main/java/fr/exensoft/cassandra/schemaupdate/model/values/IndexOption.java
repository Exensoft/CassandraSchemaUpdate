package fr.exensoft.cassandra.schemaupdate.model.values;

public enum IndexOption {
    KEYS("index_keys"),
    VALUES("index_values");

    private String value;

    IndexOption(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
