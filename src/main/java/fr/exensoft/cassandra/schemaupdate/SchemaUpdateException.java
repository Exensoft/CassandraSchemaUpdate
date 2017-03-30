package fr.exensoft.cassandra.schemaupdate;

public class SchemaUpdateException extends RuntimeException {

    public SchemaUpdateException(String message) {
        super(message);
    }

    public SchemaUpdateException(String message, Exception exception) {
        super(message, exception);
    }
}
