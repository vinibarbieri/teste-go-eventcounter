build:
	go build -o bin/main main.go

run: build
	./bin/main

clean:
	rm -rf bin/
	rm -rf data/

