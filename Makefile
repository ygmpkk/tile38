all: tile38-server tile38-cli tile38-benchmark tile38-luamemtest tile38-spring

.PHONY: tile38-server
tile38-server:
	@./scripts/build.sh tile38-server

.PHONY: tile38-cli
tile38-cli:
	@./scripts/build.sh tile38-cli

.PHONY: tile38-benchmark
tile38-benchmark:
	@./scripts/build.sh tile38-benchmark

.PHONY: tile38-luamemtest
tile38-luamemtest:
	@./scripts/build.sh tile38-luamemtest

.PHONY: tile38-spring
tile38-spring:
	@echo "Building Tile38 Spring Boot integration..."
	@cd tile38-spring && mvn clean package -DskipTests

test: all
	@./scripts/test.sh
	@echo "Testing Spring Boot integration..."
	@cd tile38-spring && mvn test

package:
	@rm -rf packages/
	@scripts/package.sh Windows windows amd64
	@scripts/package.sh Mac     darwin  amd64
	@scripts/package.sh Linux   linux   amd64
	@scripts/package.sh FreeBSD freebsd amd64
	@scripts/package.sh ARM     linux   arm
	@scripts/package.sh ARM64   linux   arm64
	@echo "Building Spring Boot JAR package..."
	@cd tile38-spring && mvn package -DskipTests
	@cp tile38-spring/target/tile38-spring-boot-starter-*.jar packages/ || true

clean:
	rm -rf tile38-server tile38-cli tile38-benchmark tile38-luamemtest 
	@cd tile38-spring && mvn clean || true

distclean: clean
	rm -rf packages/

install: all
	cp tile38-server /usr/local/bin
	cp tile38-cli /usr/local/bin
	cp tile38-benchmark /usr/local/bin

install-spring: tile38-spring
	@echo "Installing Spring Boot integration..."
	@cd tile38-spring && mvn install

uninstall: 
	rm -f /usr/local/bin/tile38-server
	rm -f /usr/local/bin/tile38-cli
	rm -f /usr/local/bin/tile38-benchmark

# Docker targets for the hybrid Go/Java architecture
docker-build:
	docker build -t tile38/tile38-go .
	docker build -t tile38/tile38-spring ./tile38-spring

docker-compose-up:
	cd tile38-spring && docker-compose up --build

docker-compose-down:
	cd tile38-spring && docker-compose down
