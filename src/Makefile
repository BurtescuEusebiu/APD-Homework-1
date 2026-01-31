build:
	mkdir -p ../target
	javac -cp "./lib/jackson-core-2.9.9.jar:./lib/jackson-databind-2.9.9.jar:./lib/jackson-annotations-2.9.9.jar" ./main/java/*.java -d ../target

run:
	java -cp "./lib/jackson-core-2.9.9.jar:./lib/jackson-databind-2.9.9.jar:./lib/jackson-annotations-2.9.9.jar:../target" Tema1 $(ARGS)

clean:
	rm -rf ../target
	rm -rf *.txt
