package fr.exensoft.cassandra.schemaupdate.comparator.delta;

public interface DeltaPriorities {

    public final static int DROP_TABLE = -50;

    public final static int CREATE_TABLE = 0;

    public final static int RENAME_TABLE = -1;

    public final static int CREATE_COLUMN = 10;

    public final static int DROP_COLUMN = -10;

    public final static int RENAME_COLUMN = 1;

    public final static int ALTER_TYPE = 5;

    public final static int CREATE_INDEX = 20;

    public final static int DROP_INDEX = -25;

    public final static int CREATE_KEYSPACE = -100;

}
