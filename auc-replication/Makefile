.PHONY: run kill-leader clean

CLIENTS ?= 3

run:
	@bash -c './create_network.sh $(CLIENTS);'

kill-leader:
	@echo "Killing Leader on port 50001..."
	@kill -TERM $$(lsof -ti:50001)
	@echo "Leader killed, Follower replica should become new Leader"

kill-follower:
	@echo "Killing Leader on port 50002..."
	@kill -TERM $$(lsof -ti:50002)
	@echo "Follower killed, nothing should change"

clean: 
	@echo "Cleaning up..."
	@kill -TERM $$(lsof -ti:50001) 2>/dev/null || true
	@kill -TERM $$(lsof -ti:50002) 2>/dev/null || true
	@rm -f ./servers/*.log ./clients/*.log
	@echo "Cleanup complete"
