hadoop fs -mkdir /Ex2


hadoop fs -put input1.txt /Ex2/input1.txt

hadoop fs -put input2.txt /Ex2/input2.txt

hadoop fs -put input3.txt /Ex2/input3.txt


javac -cp "./lib/*" Exercise2/*.java

jar cf Ex2.jar Exercise2/*.class

hadoop jar Ex2.jar Exercise2.Ex2 /Ex2/input1.txt /Ex2/input2.txt /Ex2/input3.txt /Ex2/output


hadoop fs -cat /Ex2/output/part-r-00000