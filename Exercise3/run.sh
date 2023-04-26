hadoop fs -mkdir /Ex3


hadoop fs -put input1.txt /Ex3/input1.txt

hadoop fs -put input2.txt /Ex3/input2.txt

hadoop fs -put input3.txt /Ex3/input3.txt


javac -cp "./lib/*" Exercise3/*.java

jar cf Ex3.jar Exercise3/*.class

hadoop jar Ex3.jar Exercise3.Ex3 /Ex3/input1.txt /Ex3/input2.txt /Ex3/input3.txt /Ex3/output


hadoop fs -cat /Ex3/output/part-r-00000