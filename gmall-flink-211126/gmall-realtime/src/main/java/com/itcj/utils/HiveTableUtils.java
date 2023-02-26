//package com.itcj.utils;
//
//import org.apache.flink.table.api.*;
//import org.apache.flink.table.catalog.*;
//import org.apache.flink.table.descriptors.*;
//import org.apache.flink.table.types.DataType;
//import org.apache.flink.types.Row;
//
//public class HiveTableUtils {
//
//    private final String catalogName;
//    private final String hiveConfDir;
//    private final String defaultDatabase;
//
//    public HiveTableUtils(String catalogName, String hiveConfDir, String defaultDatabase) {
//        this.catalogName = catalogName;
//        this.hiveConfDir = hiveConfDir;
//        this.defaultDatabase = defaultDatabase;
//    }
//
//    public void createTable(TableSchema schema, String tableName) {
//        // Step 1: Create a HiveCatalog
//        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir);
//
//        // Step 2: Create a TableEnvironment
//        TableEnvironment tableEnv = TableEnvironment.create(hiveCatalog);
//
//        // Step 3: Define the table using Table API
//        String dbName = hiveCatalog.getDefaultDatabase();
//        TableDescriptor tableDescriptor = new TableDescriptor(
//                new CatalogTableImpl(schema, null, null, null),
//                new CatalogTableStatistics(0L),
//                new CatalogTableConfig(),
//                null
//        );
//
//        tableEnv.createTable(
//                CatalogTable.of(
//                        new ObjectPath(dbName, tableName),
//                        tableDescriptor
//                )
//        );
//
//        // Step 4: Execute a Hive DDL statement
//        String ddl = getCreateTableDDL(dbName, tableName, schema);
//        tableEnv.executeSql(ddl);
//    }
//
//    private String getCreateTableDDL(String dbName, String tableName, TableSchema schema) {
//        StringBuilder sb = new StringBuilder();
//        sb.append("CREATE TABLE ")
//                .append(dbName)
//                .append(".")
//                .append(tableName)
//                .append(" (\n");
//
//        for (int i = 0; i < schema.getFieldCount(); i++) {
//            String fieldName = schema.getFieldNames()[i];
//            DataType fieldType = schema.getFieldDataTypes()[i];
//            String hiveType = getHiveType(fieldType);
//
//            sb.append(fieldName)
//                    .append(" ")
//                    .append(hiveType)
//                    .append(",\n");
//        }
//
//        sb.deleteCharAt(sb.length() - 2);
//        sb.append(")\n")
//                .append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n")
//                .append("STORED AS TEXTFILE");
//
//        return sb.toString();
//    }
//
//    private String getHiveType(DataType fieldType) {
//        switch (fieldType.getLogicalType().getTypeRoot()) {
//            case BOOLEAN:
//                return "BOOLEAN";
//            case INTEGER:
//                return "INT";
//            case BIGINT:
//                return "BIGINT";
//            case FLOAT:
//                return "FLOAT";
//            case DOUBLE:
//                return "DOUBLE";
//            case VARCHAR:
//                return "STRING";
//            default:
//                throw new IllegalArgumentException("Unsupported data type: " + fieldType.toString());
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        // Define the schema
//        TableSchema schema = TableSchema.builder()
//                .field("id", DataTypes.INT())
//                .field("name", DataTypes.STRING())
//                .field("age", DataTypes.INT())
//                .build();
//
//        // Create a Hive table using the schema
//        HiveTableUtils utils = new HiveTableUtils("myCatalog", "/path/to/hive/conf", "default");
//        utils.createTable(schema, "myTable");
//    }
//}
//
