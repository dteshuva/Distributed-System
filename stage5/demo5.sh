#! /bin/bash


exec &> >(tee output.log) # send output to file


mvn clean test


java -cp target/classes edu.yu.cs.com3800.stage5.demo.Demo5
