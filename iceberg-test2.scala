
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object GlueApp {
 
  def main(sysArgs: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession

    // Parse job arguments
    val args = GlueArgParser.getResolvedOptions(sysArgs, Array("JOB_NAME"))
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    // Define database and table names
    val database = "testdb"
    val table = "employees"
    val fullTable = s"glue_catalog.$database.$table"
   // val fullTable = s"$database.$table"
    
val dropTable = s"""DROP table $fullTable"""
spark.sql(dropTable)

val createTable = s"""CREATE TABLE IF NOT EXISTS $fullTable (
    employee_id BIGINT,
    employee_name STRING,
    employee_dept INTEGER,
    employee_salary DOUBLE
)
LOCATION 's3://kishan-08-28/icebergtables/employee/'
TBLPROPERTIES (
    "format-version" = "3",
    "format" = "parquet",
    "write_compression" = "zstd"
);
""".stripMargin

spark.sql(createTable)

//val truncateTable =  s"""DELETE FROM $fullTable WHERE employee_id > 0;""".stripMargin
//spark.sql(truncateTable)

val inserRecords = s"""
INSERT INTO $fullTable (employee_id, employee_name, employee_dept, employee_salary) VALUES
(1001, 'Alice Smith', 10, 85000.50),
(1002, 'Bob Jones', 20, 62000.00),
(1003, 'Charlie Brown', 10, 95000.75),
(1004, 'Diana Prince', 30, 110000.00),
(1005, 'Evan Wright', 20, 58500.25),
(1006, 'Fiona Gallagher', 40, 72000.00),
(1007, 'George Clark', 30, 88000.90),
(1008, 'Hannah Abbott', 10, 67000.00),
(1009, 'Ian Malcolm', 40, 105000.50),
(1010, 'Julia Roberts', 20, 79000.00);
"""

spark.sql(inserRecords)

val select1 = s"""
SELECT
  _row_id, _last_updated_sequence_number,employee_id, employee_name, employee_dept, employee_salary
FROM $fullTable
WHERE _last_updated_sequence_number > 0
ORDER BY _row_id,  _last_updated_sequence_number;
""".stripMargin

var df = spark.sql(select1)
df.show()

val updateTable1 =  s"""update $fullTable set employee_dept =100 WHERE employee_dept = 10;""".stripMargin

spark.sql(updateTable1)


df.show()

val updateTable2 =  s"""update $fullTable set employee_dept =200 WHERE employee_dept = 20;""".stripMargin

spark.sql(updateTable2)

df.show()

val select2 = s"""
SELECT snapshot_id, committed_at
FROM $fullTable.snapshots
ORDER BY committed_at;
"""
var df2 = spark.sql(select2)
df2.show(truncate=false)


  }
}



