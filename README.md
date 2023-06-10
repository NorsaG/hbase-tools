# HBase Tools

#### Set of useful tools for HBase administration and development


### List of available tools:
~\hbase-tools-runner\target>java -jar hbase-tools-runner-0.1.0.jar help

* COMPACTOR - tool for controlled running 'major_compact'.
* FLUSHER - tool for fast 'flush' HBase MemStore.
* MERGER - tool that merge regions inside table and decrease number of regions in table.
* DISTRIBUTOR - tool for distribute regions throughout all HBase cluster with normal distribution.
* SPLITTER - tool for table splitting (till given regions number)
* TABLE_ANALYZER - tool for table analyzing. Provide analytics for table.
* CLUSTER_ANALYZER - tool for cluster analyzing. Provide analytics for whole HBase cluster.
* KEY_GENERATOR - tool for generating default HBase keys.
* KEY_FINDER - tool for finding region for a given key.
* TABLE_CHECKER - tool that checks availability of all table regions.
* TABLE_COPY - tool that creates copy of a given table.
* TABLE_REPLACE - tool that replace one table by another
* REPORT - show report about cluster state
* HEALTH_CHECK - check HBase cluster availability
* META - check meta (unstable)
* COMPACTION_CLEANER - cleanup all compaction queues on each RS




## List of issues:

* Rework all code for HBase 2.x API usages (remove deprecated HBase API)
* SequenceBufferedMutator implementation
* Compactor threads model
* Cluster topology model and restore checks
* Logging model
* Unit testing 