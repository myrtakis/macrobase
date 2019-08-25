JAVA_COMMAND = java -jar alexp/target/benchmark.jar
CONFIG_PATH = alexp/data/explanation/experiments/refout_datasets/experiment1_a/lookout_configs/breast_lof/5D_B50/iforest
OUTPUT_PATH = alexp/explanationExp/refout_datasets/experiment1_a
BENCH_COMMAND = "$(JAVA_COMMAND) -b $(CONFIG_PATH) --e --so $(OUTPUT_PATH)"
all:
	tmux new -d -s mod_refout_data_exp1a_lookout_31_5d_iforest
	tmux send-keys -t mod_refout_data_exp1a_lookout_31_5d_iforest.0 "cd ~/Documents/macrobase" ENTER
	tmux send-keys -t mod_refout_data_exp1a_lookout_31_5d_iforest.0 $(BENCH_COMMAND) ENTER
