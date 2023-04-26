hadoop fs -mkdir /Ex4


hadoop fs -put input1.txt /Ex4/input1.txt

hadoop fs -put input2.txt /Ex4/input2.txt

hadoop fs -put input3.txt /Ex4/input3.txt


javac -cp "./lib/*" Exercise4/*.java

jar cf Ex4.jar Exercise4/*.class

hadoop jar Ex4.jar Exercise4.Ex4 /Ex4/input1.txt /Ex4/input2.txt /Ex4/input3.txt /Ex4/output


hadoop fs -cat /Ex4/output/part-r-00000