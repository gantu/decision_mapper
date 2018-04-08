package kg.baaber

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.io.StdIn.readLine
import scala.io.Source
import scala.util.parsing.json._

object DataSummary{
	
	case class dTypes(ecn:String,ncn:String,ndt:String,de:String)
	case class DfSummary(name:String, uniqueValues:Long,values:List[Any])

	def main(args:Array[String]){
		val spark = SparkSession.builder
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate;

        

		val filePath = args(0)
		if(args.length>0){
         	//Step 1 read from CSV and convert it into DataFrame with the header
         	val dataRDD = spark.sparkContext.textFile(args(0))

         	val cleanData = dataRDD.map(line => line.split(',').map(_.trim.replaceAll(" +", "") filterNot(x=>(x == '‘' || x == '’'))))
         	val headerColumns = cleanData.first().to[List]
         	val fields = headerColumns.map { x=> StructField(name = x, dataType = StringType, nullable = true)}
         	
         	def dfSchema(field:List[StructField]): StructType = {StructType(field)}
            val schema = dfSchema(fields)
         	
         	val data = cleanData.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(x=> Row.fromSeq(x))
         	val dataNonEmpty = data.filter(x=> x.length>1)
         	val dataDF = spark.createDataFrame(dataNonEmpty,schema)
         	
         	dataDF.show()


         	
         	//Step 2 filter empty Strings getOrCreate
         	val filterCond = dataDF.columns.map( x => col(x) !=="").reduce(_ && _)
         	val filteredDf = dataDF.filter(filterCond)
         	
         	filteredDf.show()

         	//Step3 change DF column names and data types according to user input

	        println("input data types file path")

	        val filename = getUserInput
	        val lines = Source.fromFile(filename).getLines.mkString
	        val jsonData = JSON.parseFull(lines)
	        val result = jsonData match {
	        	case Some(l:List[Map[String,Any]]) => l
	        	case None => List() 
	        }

			val d = result.map{x=>
				val ecn = x.get("existing_col_name") match{
					case Some(e) => e.toString
					case None => ""
				}

				val ncn = x.get("new_col_name") match{
					case Some(e) => e.toString
					case None => ""
				}
				val ndt = x.get("new_data_type") match{
					case Some(e) => e.toString
					case None => ""
				}
				val de = x.get("date_expression") match{
					case Some(e) => e.toString
					case None => ""
				}
				dTypes(ecn,ncn,ndt,de)
			}


			val newDF = d.foldLeft(filteredDf){case(tempdf, x) => x.ndt match {
				case "integer" => tempdf.withColumn(x.ecn, col(x.ecn).cast(x.ndt))
				case "double" => tempdf.withColumn(x.ecn, col(x.ecn).cast(x.ndt))
				case "date" => tempdf.withColumn(x.ecn, to_date(col(x.ecn),x.de))
				case _ =>  tempdf.withColumn(x.ecn, col(x.ecn).cast(x.ndt))
			}}

	        
			newDF.printSchema()
	        newDF.show()

	        //val resultDF = d.foldLeft(newDF){case(tempdf,x) => tempdf.withColumnRenamed(x.ecn,x.ncn)}
	        val cols = d.map(x=> newDF.col(x.ecn).alias(x.ncn))
	        val resultDF = newDF.select(cols:_*)

	        resultDF.printSchema()
	        resultDF.show()

	        //Step 4 : very inefficient solution
	        val summary:Array[DfSummary]=resultDF.columns.map(x=> DfSummary(x,resultDF.select(col(x)).distinct.count,resultDF.select(col(x)).collect().map(_(0)).toList))
	        
	        summary.foreach{x =>
	        	val s = s"ColumnName:${x.name},\nUnique_values:${x.uniqueValues},\nValues [${x.values.map(y =>y match{
	        		case e:Any => e.toString
	        		case null => ""
	        	})}"
	        	println(s)
	        }
		}

		def getUserInput(): String = readLine.trim
		

	}



	

}