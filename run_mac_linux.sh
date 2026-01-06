#!/bin/bash
mkdir -p bin
javac -d bin -encoding UTF-8 src/bdda/core/*.java src/bdda/storage/*.java src/bdda/query/*.java src/bdda/manager/*.java src/bdda/sgbd/*.java src/bdda/tests/*.java
java -cp bin bdda.sgbd.SGBD config/config.txt
