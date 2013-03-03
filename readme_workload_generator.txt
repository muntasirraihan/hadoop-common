High Level Use:

	Input: -input <.tsv file> (A tsv file for job trace information, one job per line)
	Output: A scaled hadoop job for each job in the tsv file
	Additional input parameters:
		Queue parameters: -queue <queuename> <multiplier> <mapNormalize> <reduceNormalize>
		MinMaps, Min Reduces
		Run option: -run "analyze" or "generate" (creates output script file) [this is what we need mainly]
		SLEEP_Normalize: -sleep_normalize (how to scale the sleep time between jobs)
		HDFS_INPUT: -num_hdfs_inputs ()

Details:

	Important methods:
	
	runAnalyze() 

	runGenerate()  // this is what we need

	{
		At the start, this method creates the initial part of the script that calls "run-jobs-script.sh" with some options:
			e.g., --suspend-strategy shortest, --hdfs-input workGenInput, --interval 9, 
			--hdfs-input-num (this input comes from the command line )

		For (each input line in the tsv [one per job]) {
			extract the job_name [column 0]
			
			extract queuename, and get queue parameters from QueueScaling object
			
			extract submit_timestamp
			
			the sleep variable keeps track of the difference between subsequent job submit_timestamps
			the sleep variable is scaled by sleep_Normalize (from command line): sleep /= sleep_normalize
			
			slotMillis(Maps|Reduces) extracted // this represents total time taken by all maps/ all reduces in a run

			map/reduceRatio = slotsMillis(Maps|Reduces) / queueScaling.(map|reduce)Normalize

			numMapsOrig, numReducesOrig extracted from tsv file
			
			both of these mutliplied by queueScaling.multiplier (result saved in numMaps, numReduces)

			mapRatio /= numMaps  // average time for one map
			reduceRatio /= numReduces // average time for one reduce

			One special case handled (only if minMaps, minReduces specified in command line)

			job parameters added to script output:
				--sleep, --queue, --mapRatio, --reduceRatio, numMaps, numReduces, job, jobName	
			counter "job" keeps track of job count
			totalMaps keeps track of total maps in all jobs		
		}
		
		Add aditional lines to script to check for the total number of running jobs and wait until the active job count is 0
		(Uses the "hadoop job -list" command)

		Finally add one line to script to sleep for a long duration

		Return; // to create the script the java program output must be redirected to a .sh output file
	}

	parseQueues()
	{
		// This method takes each queue input tuple, e.g, [-queue research 8.0 87500 87500], and adds an entry to a map:
		map entry, key = queue name, value = <multiplier, mapNormalize, reduceNormalize>
	}
