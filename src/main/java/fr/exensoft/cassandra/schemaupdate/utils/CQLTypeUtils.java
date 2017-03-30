package fr.exensoft.cassandra.schemaupdate.utils;


import fr.exensoft.cassandra.schemaupdate.model.type.*;

import java.util.Arrays;
import java.util.List;

public class CQLTypeUtils {

    private static ColumnType simplifyType(ColumnType type) {
        if(type == BasicType.VARCHAR) {
            return BasicType.TEXT;
        }

        return type;
    }

    public static boolean isCompatible(ColumnType type1, ColumnType type2) {
        if((type1 instanceof BasicType) && (type2 instanceof BasicType)) {
            type1 = simplifyType(type1);
            type2 = simplifyType(type2);

            if(type1.equals(type2)) {
                return true;
            }

            // (ascii, bigint, boolean, decimal, double, float, inet, int, timestamp, timeuuid, uuid, varchar, varint) -> blob
            if(type2.equals(BasicType.BLOB)) {
                return Arrays.asList(BasicType.ASCII, BasicType.BIGINT, BasicType.BOOLEAN, BasicType.DECIMAL, BasicType.DOUBLE,
                        BasicType.FLOAT, BasicType.INET, BasicType.INT, BasicType.TIMESTAMP, BasicType.TIMEUUID, BasicType.UUID,
                        BasicType.VARCHAR, BasicType.VARINT, BasicType.TEXT).contains(type1);
            }

            //int -> varint
            if(type2.equals(BasicType.VARINT) && type1.equals(BasicType.INT)) {
                return true;
            }

            //timeuuid -> uuid
            if(type2.equals(BasicType.UUID) && type1.equals(BasicType.TIMEUUID)) {
                return true;
            }

            return false;
        }
        else if(type1.getClass() != type2.getClass()){
            return false;
        }
        else if(type1 instanceof OneParameterType) {
            OneParameterType list1 = (OneParameterType) type1;
            OneParameterType list2 = (OneParameterType) type2;
            return isCompatible(list1.getInnerType(), list2.getInnerType());
        }
        else if(type1 instanceof MapType) {
            MapType list1 = (MapType) type1;
            MapType list2 = (MapType) type2;
            return equals(list1.getKeyType(), list2.getKeyType()) && isCompatible(list1.getValueType(), list2.getValueType());
        }
        else {
            return false;
        }
    }

    public static boolean isOrderCompatible(ColumnType type1, ColumnType type2) {
        if((type1 instanceof BasicType) && (type2 instanceof BasicType)) {
            type1 = simplifyType(type1);
            type2 = simplifyType(type2);

            if(type1.equals(type2)) {
                return true;
            }
            //int -> varint
            if(type2.equals(BasicType.VARINT) && type1.equals(BasicType.INT)) {
                return true;
            }

            return false;
        }
        else if(type1.getClass() != type2.getClass()){
            return false;
        }
        else if(type1 instanceof OneParameterType) {
            OneParameterType list1 = (OneParameterType) type1;
            OneParameterType list2 = (OneParameterType) type2;
            return isOrderCompatible(list1.getInnerType(), list2.getInnerType());
        }
        else if(type1 instanceof MapType) {
            MapType list1 = (MapType) type1;
            MapType list2 = (MapType) type2;
            return equals(list1.getKeyType(), list2.getKeyType()) && isOrderCompatible(list1.getValueType(), list2.getValueType());
        }
        else {
            return false;
        }
    }

    public static boolean equals(ColumnType type1, ColumnType type2) {
        if((type1 instanceof BasicType) && (type2 instanceof BasicType)) {
            return simplifyType(type1).equals(simplifyType(type2));
        }
        else if(type1.getClass() != type2.getClass()){
            return false;
        }
        else if(type1 instanceof OneParameterType) {
            OneParameterType list1 = (OneParameterType) type1;
            OneParameterType list2 = (OneParameterType) type2;
            return equals(list1.getInnerType(), list2.getInnerType());
        }
        else if(type1 instanceof MapType) {
            MapType list1 = (MapType) type1;
            MapType list2 = (MapType) type2;
            return equals(list1.getKeyType(), list2.getKeyType()) && equals(list1.getValueType(), list2.getValueType());
        }
        else {
            return false;
        }
    }

}
