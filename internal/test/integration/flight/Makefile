build-client:
	-rm client/c_test
	cd client && RUST_BACKTRACE=1 RUST_LOG=debug go test -o c_test -v

build-server:
	-rm server/server
	cd server && go build

start-server:
	cd server && RUST_BACKTRACE=1 RUST_LOG=debug ./server

.PHONY: build-server
