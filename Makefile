all:clean
	@go build -v -o bin/canald ./canald
	@go build -v -o bin/canalctl ./canalctl
	@cp ./canald/canald.toml bin/
	@cp ./canalctl/dump.toml bin/
clean:
	@rm -rf bin