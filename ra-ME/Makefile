.PHONY: run clean

NODES ?= 3

run: 
	@bash -c './create_network.sh $(NODES);'

clean: 
	@bash -c 'kill -TERM $$(lsof -t -i:50001);'