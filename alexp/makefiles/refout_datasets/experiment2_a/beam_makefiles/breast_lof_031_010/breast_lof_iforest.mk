JAVA_COMMAND = java -jar alexp/target/benchmark.jar
CONFIG_PATH = alexp/data/explanation/experiments/refout_datasets/experiment2_a/beam_configs/breast_lof/iforest
OUTPUT_PATH = alexp/explanationExp/refout_datasets/experiment2_a
BENCH_COMMAND = "$(JAVA_COMMAND) -b $(CONFIG_PATH) --e --so $(OUTPUT_PATH)"
all:
	tmux new -d -s refout_data_exp2a_beam_31_iforest
	tmux send-keys -t refout_data_exp2a_beam_31_iforest.0 "cd ~/Documents/macrobase" ENTER
	tmux send-keys -t refout_data_exp2a_beam_31_iforest.0 $(BENCH_COMMAND) ENTER
