hadoop fs -mkdir /Code_20120623


hadoop fs -put input1.txt /Code_20120623/input1.txt

hadoop fs -put input2.txt /Code_20120623/input2.txt

hadoop fs -put input3.txt /Code_20120623/input3.txt


javac -cp "./lib/*" submission_20120623/*.java

jar cf Code_20120623.jar submission_20120623/*.class

hadoop jar Code_20120623.jar submission_20120623.Code_20120623 /Code_20120623/input1.txt /Code_20120623/input2.txt /Code_20120623/input3.txt /Code_20120623/output


hadoop fs -cat /Code_20120623/output/part-r-00000