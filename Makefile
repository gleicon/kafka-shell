deps:
	make -C cmd/ktail deps
	make -C cmd/ktee deps

all:
	make -C cmd/

install:
	@cp cmd/ktail/ktail bin/
	@cp cmd/ktee/ktee bin/

clean:
	make -C cmd/ktail clean
	make -C cmd/ktee clean
	@rm -f bin/ktail
	@rm -f bin/ktee

