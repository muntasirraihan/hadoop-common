High Level Use:

	Input: A tsv file for job trace information, one job per line
	Output: A scaled hadoop job for each job in the tsv file
	Additional input parameters:
		Queue parameters: -queue <queuename> <multiploer> <mapNormalize> <reduceNormalize>
		MinMaps, Min Reduces
		Run option: -run "analyze" or "generate" (creates output script file) [this is what we need mainly]
		SLEEP_Normalize: -sleep_normalize (how to scale the sleep time between jobs)
		HDFS_INPUT: -num_hdfs_inputs ()
