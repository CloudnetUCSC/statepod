
.PHONY: build nrand run test clean

all: build
build: nrand
	@echo "Building statepod..."
	@mkdir -p bin
	go build -o bin/statepod cmd/statepod/main.go
nrand:
	@echo "Building nrand helper..."
	@mkdir -p bin
	gcc -static -O2 -o bin/nrand tools/nrand/nrand.c
run: build
	@echo "Running statepod..."
	./bin/statepod
tidy:
	@echo "Tidying up dependencies..."
	go mod tidy
test:
	@echo "Running tests..."
	go test -v ./...
clean:
	@echo "Cleaning up..."
	rm -rf bin
