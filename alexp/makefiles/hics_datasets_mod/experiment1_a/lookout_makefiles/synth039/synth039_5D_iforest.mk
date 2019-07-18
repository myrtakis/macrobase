JAVA_COMMAND = java -jar alexp/target/benchmark.jar
CONFIG_PATH = alexp/data/explanation/experiments/hics_mod_datasets/experiment1_a/lookout_configs/synthmod039/5D_B50/iforest
OUTPUT_PATH = alexp/explanationExp/hics_datasets_modified/experiment1_a
BENCH_COMMAND = "$(JAVA_COMMAND) -b $(CONFIG_PATH) --e --so $(OUTPUT_PATH)"
all:
	tmux new -d -s mod_hics_data_exp1a_lookout_39_5d_iforest
	tmux send-keys -t mod_hics_data_exp1a_lookout_39_5d_iforest.0 "cd ~/Documents/macrobase" ENTER
	tmux send-keys -t mod_hics_data_exp1a_lookout_39_5d_iforest.0 $(BENCH_COMMAND) ENTER
