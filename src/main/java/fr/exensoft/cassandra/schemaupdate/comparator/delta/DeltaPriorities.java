package fr.exensoft.cassandra.schemaupdate.comparator.delta;

public interface DeltaPriorities {

    int DROP_TABLE = -50;

    int CREATE_TABLE = 0;

    int RENAME_TABLE = -1;

    int CREATE_COLUMN = 10;

    int DROP_COLUMN = -10;

    int RENAME_COLUMN = 1;

    int ALTER_TYPE = 5;

    int CREATE_INDEX = 20;

    int DROP_INDEX = -25;

    int CREATE_KEYSPACE = -100;

}
