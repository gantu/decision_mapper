Spark Scala Test

compile: 

sbt> package 

run: 

spark-submit --master "local[*]" --class kg.baaber.DataSummary target/scala-2.11/sparkme-project_2.11-1.0.jar {path to the input csv file}


Within the program it will ask for data types json file path for Step 4. 