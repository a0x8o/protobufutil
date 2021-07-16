// Databricks notebook source
// MAGIC %md
// MAGIC # Native Kafka Protobuf consumption
// MAGIC Install ```protobufutility-1.0-SNAPSHOT.jar``` and ```addressbook-1.0-SNAPSHOT.jar``` on your cluster

// COMMAND ----------

// MAGIC %md
// MAGIC #### It is critical to:
// MAGIC 
// MAGIC ##### 1: shade the com.google.protobuf classes, for example:
// MAGIC 
// MAGIC     ```
// MAGIC     <plugin>
// MAGIC       <groupId>org.apache.maven.plugins</groupId>
// MAGIC       <artifactId>maven-shade-plugin</artifactId>
// MAGIC       <version>3.2.4</version>
// MAGIC       <executions>
// MAGIC         <execution>
// MAGIC           <phase>package</phase>
// MAGIC           <goals>
// MAGIC             <goal>shade</goal>
// MAGIC           </goals>
// MAGIC           <configuration>
// MAGIC             <relocations>
// MAGIC               <relocation>
// MAGIC                 <pattern>com.google.protobuf</pattern>
// MAGIC                 <shadedPattern>com.example.shade.google.protobuf</shadedPattern>
// MAGIC               </relocation>
// MAGIC             </relocations>
// MAGIC           </configuration>
// MAGIC         </execution>
// MAGIC       </executions>
// MAGIC     </plugin>    
// MAGIC     ```
// MAGIC   
// MAGIC 
// MAGIC ##### 2: set the com.google.protobuf.protobuf-java and com.google.protobuf.protobuf-java-util dependencies, using the same version of protobuf as installed on the cluster:
// MAGIC 
// MAGIC     ```
// MAGIC     <dependency>
// MAGIC       <groupId>com.google.protobuf</groupId>
// MAGIC       <artifactId>protobuf-java</artifactId>
// MAGIC       <version>3.14.0</version>
// MAGIC     </dependency>
// MAGIC     <dependency>
// MAGIC       <groupId>com.google.protobuf</groupId>
// MAGIC       <artifactId>protobuf-java-util</artifactId>
// MAGIC       <version>3.14.0</version>
// MAGIC     </dependency>
// MAGIC     ```

// COMMAND ----------

import com.example.protobuf.ProtobufUtility

// COMMAND ----------

import com.example.protos._; // AddressBook protobuf

// COMMAND ----------

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.spark.sql.types.{StructType,ArrayType,DataType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Create ProtoBuf format data
val msg1 =  AddressBook.newBuilder()
                .addPeople(
                        Person.newBuilder().
                setEmail("alice@outlook.com")
                .setId(0)
                .setName("Alice").
                                addPhones(
                                Person.PhoneNumber.newBuilder()
                                        .setNumber("8308843643")
                                        .setType(Person.PhoneType.HOME))
                ).build();


val msg2 =  AddressBook.newBuilder()
                .addPeople(
                        Person.newBuilder()
                                .setEmail("bob@gmail.com")
                                .setId(1)
                                .setName("Bob")
                                .addPhones(
                                        Person.PhoneNumber.newBuilder()
                                                .setNumber("3434343434")
                                                .setType(Person.PhoneType.MOBILE))
                ).build();

val msg3 =  AddressBook.newBuilder()
                .addPeople(
                        Person.newBuilder()
                                .setEmail("max@gmail.com")
                                .setId(1)
                                .setName("max")
                                .addPhones(
                                        Person.PhoneNumber.newBuilder()
                                                .setNumber("1234567890")
                                                .setType(Person.PhoneType.MOBILE))
                ).build();

val msg1Bytes = msg1.toByteArray();
val msg2Bytes = msg2.toByteArray();
val msg3Bytes = msg3.toByteArray();



// COMMAND ----------

// DBTITLE 1,Create dataframe on ProtoBuf Data - This can from kafka / Text file or any other source 
val rawmessages = Seq(msg1Bytes,msg2Bytes,msg3Bytes)
val raw = sc.parallelize(rawmessages).map(x=>("notebook",x)).toDF("topic","protomsg")
raw.createOrReplaceTempView("raw")
display(raw)

// COMMAND ----------

// DBTITLE 1,Generic function to Convert ProtoBuf message to JSON - Dep on attached Jar 
import org.apache.spark.sql.functions.{col, udf}
val getProtoJsonValue = (topic: String, msg : Array[Byte] ) => {
  
  topic match {
  case "notebook" => 
     ProtobufUtility.convertToJson("com.example.protos.AddressBook", msg);
  case _ => "None"
}

}
spark.udf.register("getProtoJsonValue", getProtoJsonValue)

// COMMAND ----------

// DBTITLE 1,Use registered function to convert ProtoBuf to JSON and save in delta 
spark.sql("select * , getProtoJsonValue(topic,protomsg) as jsonmessage from raw").write.mode(SaveMode.Overwrite).format("delta").saveAsTable("notebook_data")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from notebook_data;

// COMMAND ----------

// DBTITLE 1,Generic function to flatten the schema 

def flattenDataframe(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    
    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }


// COMMAND ----------

// DBTITLE 1,Build the Schema for Person
val personSchema: String = """{
  "type" : "struct",
  "fields" : [ {
    "name" : "name",
    "type" : "string",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "id",
    "type" : "integer",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "email",
    "type" : "string",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name": "phones",
    "type" : "string",
    "nullable" : false,
    "metadata" : { }  
  } ]
}"""

val personSchema2: String = """{
  "type" : "struct",
  "fields" : [ {
    "name" : "name",
    "type" : "string",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "id",
    "type" : "integer",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "email",
    "type" : "string",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "phones",
    "type" : {
      "type" : "struct",
      "fields" : [ {
        "name" : "number",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "type",
        "type" : "integer",
        "nullable" : true,
        "metadata" : { }
      } ]
    },
    "nullable" : true,
    "metadata" : { } 
  } ]
}"""

val personSchema3: String = """{
  "type" : "struct",
  "fields" : [ {
    "name" : "name",
    "type" : "string",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "id",
    "type" : "integer",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "email",
    "type" : "string",
    "nullable" : false,
    "metadata" : { }
  }, {
    "name" : "phones",
    "type" : {
      "type" : "struct",
      "fields" : [ {
        "name" : "number",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      }, {
        "name" : "type",
        "type" : "string",
        "nullable" : true,
        "metadata" : { }
      } ]
    },
    "nullable" : true,
    "metadata" : { } 
  } ]
}"""

val personSchemaFromProtobuf: String = """{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "required": [
        "name",
        "id",
        "email",
        "phones"
    ],
    "properties": {
        "name": {
            "type": "string"
        },
        "id": {
            "type": "integer"
        },
        "email": {
            "type": "string"
        },
        "phones": {
            "items": {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "required": [
                    "number",
                    "type"
                ],
                "properties": {
                    "number": {
                        "type": "string"
                    },
                    "type": {
                        "enum": [
                            "MOBILE",
                            0,
                            "HOME",
                            1,
                            "WORK",
                            2
                        ],
                        "oneOf": [
                            {
                                "type": "string"
                            },
                            {
                                "type": "integer"
                            }
                        ]
                    }
                },
                "additionalProperties": true,
                "type": "object"
            },
            "type": "array"
        }
    },
    "additionalProperties": true,
    "type": "object"
}"""

// COMMAND ----------

// DBTITLE 1,Read Schema from the location and use it to parser the JSON message and save in parsed Delta table 
val jsonString = scala.io.Source.fromFile("/dbfs/FileStore/tables/addressbook.schema", "utf-8").getLines.mkString
//val newSchema = DataType.fromJson(jsonString).asInstanceOf[StructType]
val newSchema = DataType.fromJson(personSchema3).asInstanceOf[StructType]

val pdata = spark.sql("select  jsonmessage from notebook_data ").select(from_json($"jsonmessage", newSchema) as "data")
val flattendedJSON = flattenDataframe(pdata)
display(flattendedJSON)



// save the parseed data in parsed table 
flattendedJSON.write.mode(SaveMode.Overwrite).format("delta").option("mergeSchema", "true").saveAsTable("notebook_parsed_data")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from notebook_parsed_data

// COMMAND ----------

// DBTITLE 1,How to Create Schema and Persist from JSON
// Save sample Json in file , read content as Json and, generate the schema 
val sampleSchema = spark.read.json("dbfs:/FileStore/tables/peoplesample.json").schema

// convert schema top Json 
val jsonString = sampleSchema.json

// save json to location 

dbutils.fs.put("dbfs:/FileStore/tables/people.schema", jsonString,true)



// COMMAND ----------

// DBTITLE 1,Read Schema from location and create Schema object 
import org.apache.spark.sql.types.{DataType, StructType}
val jsonString = scala.io.Source.fromFile("/dbfs/FileStore/tables/people.schema", "utf-8").getLines.mkString

val newSchema = DataType.fromJson(jsonString).asInstanceOf[StructType]
print(newSchema.printTreeString)



