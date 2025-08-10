all: tile38-server tile38-cli tile38-benchmark tile38-luamemtest java-integration

java-integration:
	@echo "Building Java Enterprise Integration with JDK 21 and ZGC..."
	@cd java-integration && \
		export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64 && \
		export PATH=$$JAVA_HOME/bin:$$PATH && \
		mvn clean package -DskipTests

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

test: all
	@./scripts/test.sh
	@echo "Running Java Enterprise Integration tests..."
	@cd java-integration && \
		export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64 && \
		export PATH=$$JAVA_HOME/bin:$$PATH && \
		mvn test

package:
	@rm -rf packages/
	@scripts/package.sh Windows windows amd64
	@scripts/package.sh Mac     darwin  amd64
	@scripts/package.sh Linux   linux   amd64
	@scripts/package.sh FreeBSD freebsd amd64
	@scripts/package.sh ARM     linux   arm
	@scripts/package.sh ARM64   linux   arm64

clean:
	rm -rf tile38-server tile38-cli tile38-benchmark tile38-luamemtest 
	@cd java-integration && mvn clean 2>/dev/null || true 

distclean: clean
	rm -rf packages/

install: all
	cp tile38-server /usr/local/bin
	cp tile38-cli /usr/local/bin
	cp tile38-benchmark /usr/local/bin
	cp start-enterprise.sh /usr/local/bin/tile38-enterprise

uninstall: 
	rm -f /usr/local/bin/tile38-server
	rm -f /usr/local/bin/tile38-cli
	rm -f /usr/local/bin/tile38-benchmark
	rm -f /usr/local/bin/tile38-enterprise
