hadoop fs -mkdir /Ex8


hadoop fs -put input1.txt /Ex8/input1.txt

hadoop fs -put input2.txt /Ex8/input2.txt

hadoop fs -put input3.txt /Ex8/input3.txt


javac -cp "./lib/*" Exercise8/*.java

jar cf Ex8.jar Exercise8/*.class

hadoop jar Ex8.jar Exercise8.Ex8 /Ex8/input1.txt /Ex8/input2.txt /Ex8/input3.txt /Ex8/output


hadoop fs -cat /Ex8/output/part-r-00000