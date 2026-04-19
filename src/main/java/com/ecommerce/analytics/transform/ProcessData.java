package com.ecommerce.analytics.transform;

import com.ecommerce.analytics.main.Main;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class ProcessData {

    private static final Logger log = LoggerFactory.getLogger(ProcessData.class);

    /**
     * Trimming the columns to remove any leading or trailing whiteSpaces,
     * Filter the columns so that any null or empty string value got dropped,
     * drop the duplicate records based on the columns
     * return the final Dataset.
     * @param data: Dataset<Row>
     * @param columns : String[]
     * @return Dataset<Row>
     */
    public static Dataset<Row> removeDuplicate(Dataset<Row> data, String[] columns){
        /*
         * Trimming the columns to remove any leading or trailing whiteSpaces;
         */
        log.info("trim the column names so leading or trailing whitespaces can be removed");
        for(String col: columns){
            data = data.withColumn(col,functions.trim(functions.column(col)));
        }
        /*
         * Filter the columns so that any null or empty string value got dropped
         */
        log.info("filtering the null/empty rows from data set");
        for(String col: columns){
            data = data.filter(functions.col(col).isNotNull().and(functions.col(col).notEqual("")));
        }
        /*
         * drop the duplicate records based on the columns
         */
        return data.dropDuplicates(columns);

    }

    /**
     * Select the columns from the whole Dataset and return the final Dataset.
     * @param data : Dataset<Row>
     * @param columnNames : String[] the columns to be selected
     * @return Dataset<Row> the final column selected Dataset.
     */
    public static Dataset<Row> selectColumns(Dataset<Row> data, String[] columnNames){
        org.apache.spark.sql.Column[] columns = new Column[columnNames.length];
        log.info("forming column array to select the columns");
        int count = 0;
        for(String col: columnNames){
            columns[count++] = new Column(col);
        }
        return data.select(columns);
    }

    /**
     * Trimming the columns to remove any leading or trailing whiteSpaces;
     * Filter the columns so that any null or empty string value got dropped
     * @param data : Dataset<Row>
     * @param columns : String[] the column names
     * @return Dataset<Row>
     */
    public static Dataset<Row> removeNullOrEmpty(Dataset<Row> data, String[] columns){
        /*
         * Trimming the columns to remove any leading or trailing whiteSpaces;
         */
        log.info("removeNullOrEmpty: trim the column names so leading or trailing whitespaces can be removed");
        for(String col: columns){
            data = data.withColumn(col,functions.trim(functions.column(col)));
        }
        /*
         * Filter the columns so that any null or empty string value got dropped
         */
        log.info("removeNullOrEmpty: filtering the null/empty rows from data set");
        for(String col: columns){
            data = data.filter(functions.col(col).isNotNull().and(functions.col(col).notEqual("")));
        }
        return data;
    }

    /**
     * Convert the columns un schema to their respective types and select the columns and return the typed selected column Dataset.
     * @param data Dataset contains data with header
     * @param schema The StructType contains column name and type.
     * @return Dataset
     * @throws Exception column mismatch/column not found/data type not convertable
     */

    public static Dataset<Row> dataTypeConversion(Dataset<Row> data, StructType schema) throws Exception {
        log.info("staring data conversion");
        Set<String> dfColumns = new HashSet<>(Arrays.asList(data.columns()));

        Set<String> integerTypes = new HashSet<>(Arrays.asList("integer","int"));
        Set<String> doubleTypes = new HashSet<>(Arrays.asList("double","float","decimal","long"));
        Set<String> dateTypes = new HashSet<>(List.of("date"));
        Set<String> timestampTypes = new HashSet<>(List.of("timestamp"));

        List<Column> columns = new ArrayList<>();

        for (StructField field : schema.fields()) {

            String colName = field.name().trim();
            String dtype = field.dataType().typeName();

            Column colExpr;

            if (dfColumns.contains(colName)) {

                Column baseCol = trim(col(colName)); // clean strings

                if (integerTypes.contains(dtype)) {
                    colExpr = baseCol.cast(DataTypes.IntegerType);

                } else if (doubleTypes.contains(dtype)) {
                    colExpr = baseCol.cast(DataTypes.DoubleType);

                } else if (dateTypes.contains(dtype)) {
                    colExpr = to_date(baseCol, "yyyy-MM-dd");

                } else if (timestampTypes.contains(dtype)) {
                    colExpr = to_timestamp(baseCol, "yyyy-MM-dd HH:mm:ss");

                } else {
                    colExpr = baseCol.cast(DataTypes.StringType);
                }

            } else {
                log.error("{} column is not found",colName);
                throw new Exception("Mismatch is filed name, field name "+ colName +" not found");

            }

            columns.add(colExpr.alias(colName));
        }

        return data.select(columns.toArray(new Column[0]));
    }
}
