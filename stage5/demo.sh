#! /bin/bash

# Daniel Teshuva

exec &> >(tee output.log)


mvn clean test


java -cp target/classes edu.yu.cs.com3800.stage5.demo.Demo
