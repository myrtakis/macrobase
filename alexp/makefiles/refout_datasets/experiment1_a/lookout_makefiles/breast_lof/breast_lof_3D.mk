JAVA_COMMAND = java -jar alexp/target/benchmark.jar
CONFIG_PATH = alexp/data/explanation/experiments/refout_datasets/experiment1_a/lookout_configs/breast_lof/3D_B50
OUTPUT_PATH = alexp/explanationExp/refout_datasets/experiment1_a
BENCH_COMMAND = "$(JAVA_COMMAND) -b $(CONFIG_PATH) --e --so $(OUTPUT_PATH)"
all:
	tmux new -d -s mod_refout_data_exp1a_lookout_31_3d
	tmux send-keys -t mod_refout_data_exp1a_lookout_31_3d.0 "cd ~/Documents/macrobase" ENTER
	tmux send-keys -t mod_refout_data_exp1a_lookout_31_3d.0 $(BENCH_COMMAND) ENTER
