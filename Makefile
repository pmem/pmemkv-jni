prefix=/usr/local

all: clean test

clean:
	rm -rf ./bin

configure:
	mkdir -p ./bin
	cd ./bin && cmake .. -DCMAKE_BUILD_TYPE=Release

sharedlib: configure
	cd ./bin && make pmemkv-jni

install:
	cp ./bin/libpmemkv-jni.so $(prefix)/lib

uninstall:
	rm -rf $(prefix)/lib/libpmemkv-jni.so

test: sharedlib
	cd ./bin && make pmemkv-jni_test
	PMEM_IS_PMEM_FORCE=1 ./bin/pmemkv-jni_test
