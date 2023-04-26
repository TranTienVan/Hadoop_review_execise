hadoop fs -mkdir /Ex7


hadoop fs -put input1.txt /Ex7/input1.txt

hadoop fs -put input2.txt /Ex7/input2.txt

hadoop fs -put input3.txt /Ex7/input3.txt


javac -cp "./lib/*" Exercise7/*.java

jar cf Ex7.jar Exercise7/*.class

hadoop jar Ex7.jar Exercise7.Ex7 /Ex7/input1.txt /Ex7/input2.txt /Ex7/input3.txt /Ex7/output


hadoop fs -cat /Ex7/output/part-r-00000