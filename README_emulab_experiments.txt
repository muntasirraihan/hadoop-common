First transfer the latest hadoop tar.gz to /proj/ISS/scheduling.

Then transfer the latest conf to /proj/ISS/scheduling.

Transfer the latest experiment to /proj/ISS/scheduling.

Create a tar.gz: tar cvzf muntasir_conf.tar.gz (The hadoop startup script uses this tar.gz).

./stop_natjam.sh 8 kills all java processes on all nodes.

Start up hadoop on the desired number of nodes: ./run_natjam_emulab.sh 8.

Run ./prepare_input.sh.

Update global.json: common->/proj/ISS/scheduling, target->/mnt/hadoop/hadoop-0.23.3-SNAPSHOT.

Now run the desired experiments using scripts in the experiment folder.
