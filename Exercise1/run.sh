javac -cp "./lib/*" Exercise1/*.java

jar cf Ex1.jar Exercise1/*.class

hadoop jar Ex1.jar Exercise1.Ex1 /Ex1/input1.txt /Ex1/input2.txt /Ex1/input3.txt /Ex1/output


hadoop fs -cat /Ex1/output/part-r-00000