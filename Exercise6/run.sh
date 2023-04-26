hadoop fs -mkdir /Ex6


hadoop fs -put input1.txt /Ex6/input1.txt

hadoop fs -put input2.txt /Ex6/input2.txt

hadoop fs -put input3.txt /Ex6/input3.txt


javac -cp "./lib/*" Exercise6/*.java

jar cf Ex6.jar Exercise6/*.class

hadoop jar Ex6.jar Exercise6.Ex6 /Ex6/input1.txt /Ex6/input2.txt /Ex6/input3.txt /Ex6/output


hadoop fs -cat /Ex6/output/part-r-00000