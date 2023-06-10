package org.evla.hbase;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.analyze.TableAnalyzeRunner;
import org.evla.hbase.common.*;
import org.evla.hbase.compactor.CompactorRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.distributor.DistributorRunner;
import org.evla.hbase.flusher.FlusherRunner;
import org.evla.hbase.merger.MergerRunner;
import org.evla.hbase.report.ReportRunner;
import org.evla.hbase.splitter.TableSplitRunner;
import org.evla.hbase.meta.MetaRunner;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public enum Tool {
    COMPACTOR {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new CompactorRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool for controlled running 'major_compact'.";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tcompactor" +
                    "\n" +
                    "\t\tInfinite compaction. Will run 'as service' and compact all required regions." +
                    "\n" +
                    "\tcompactor <namespace>|<table>" +
                    "\n" +
                    "\t\tRun controlled compactions for specified namespace|table.";
        }
    },
    FLUSHER {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new FlusherRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool for fast 'flush' HBase MemStore.";

        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tflusher" +
                    "\n" +
                    "\t\tRun 'flush' for whole HBase-cluster." +
                    "\n" +
                    "\tflusher <server>" +
                    "\n" +
                    "\t\tRun 'flush' for given server (in case of several RS on same server - for all RS`s).";
        }
    },
    MERGER {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new MergerRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool that merge regions inside table and decrease number of regions in table.";

        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tmerger <table name> <new regions number>" +
                    "\n" +
                    "\t\tRun 'merge' for given table. Merge regions till specified regions number or while HFile size less than HConstants#HREGION_MAX_FILESIZE";
        }
    },
    DISTRIBUTOR {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new DistributorRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool for distribute regions throughout all HBase cluster with normal distribution.";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tdistributor" +
                    "\n" +
                    "\t\tRisky mode!!! Can be used for your own responsibility!!! Run distributor for all tables in cluster." +
                    "\n" +
                    "\tdistributor <table name>" +
                    "\n" +
                    "\t\tRun 'distributor' for given table." +
                    "\n" +
                    "\tdistributor <HBase specific regexp> mask" +
                    "\n" +
                    "\t\tIn case of second parameter is 'mask' - run distributor for all objects of given regexp.";
        }
    },
    SPLITTER {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new TableSplitRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool for table splitting (till given regions number)";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tsplitter <table name> <new regions number>" +
                    "\n" +
                    "\t\tRun 'splitter' for given table. Split table till specified regions number.";
        }
    },
    KEY_GENERATOR {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new KeyGeneratorRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool for generating default HBase keys.";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tkey_generator <key>" +
                    "\n" +
                    "\t\tGenerate salted key for a given key.";
        }
    },
    KEY_FINDER {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new KeyFinderRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool for finding region for a given key.";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tkey_finder <unsalted key>" +
                    "\n" +
                    "\t\tGenerate salted key for given key.";
        }

    },
    TABLE_ANALYZER {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new TableAnalyzeRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool for table analyzing. Provide analytics for table.";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tanalyzer <table name>" +
                    "\n" +
                    "\t\tRun 'analyzer' for given table.";
        }
    },
    TABLE_CHECKER {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new HBaseCheckRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool that checks availability of all table regions.";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\ttable_checker <table name>" +
                    "\n" +
                    "\t\tCheck all table regions and print first and last keys in region. In case of inconsistency or another problem exception will be raised";
        }
    },
    TABLE_COPY {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new TableCopyRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool that creates copy of a given table.";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\ttable_copy <exists table> <new table name>" +
                    "\n" +
                    "\t\tCreate copy of a given table.";
        }
    },
    TABLE_REPLACE {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new TableReplaceRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - tool that replace one table by another";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\ttable_replace <from table> <to table> <true|false>" +
                    "\n" +
                    "\t\tReplace 'to'-table by 'from'-table. In case of 'true' a copy of 'to'-table will be created.";
        }
    },
    REPORT {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new ReportRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - show report about cluster state";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\treport [full]" +
                    "\n" +
                    "\t\tRun 'report' for whole cluster and provide information about hotspots" +
                    "\n" +
                    "\t\tfull - Run 'report' for whole cluster and provide full information";
        }
    },
    HEALTH_CHECK {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new HBaseHealthCheckRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - check HBase cluster availability";
        }

        @Override
        public String getToolUsageString() {
            return "Unstable tool!\n" +
                    "Usage:";
        }
    },
    META {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new MetaRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - check meta (unstable)";
        }

        @Override
        public String getToolUsageString() {
            return "Unstable tool!\n" +
                    "Usage:";
        }
    },
    COMPACTION_CLEANER {
        @Override
        public HBaseToolRunner getRunnerForTool() {
            return new HBaseCompactionQueueCleanRunner();
        }

        @Override
        public String getToolDescription() {
            return this.name() + " - cleanup all compaction queues on each RS";
        }

        @Override
        public String getToolUsageString() {
            return "Usage:" +
                    "\tcompaction_cleaner" +
                    "\n" +
                    "\t\tCleanup all compaction queues (long and short) on cluster";
        }
    };

    public abstract HBaseToolRunner getRunnerForTool();

    public abstract String getToolDescription();

    public abstract String getToolUsageString();

    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        getRunnerForTool().run(admin, settings, args);
    }

    public String getName() {
        return this.name().toLowerCase();
    }

    public static int getPID() {
        RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();
        return Integer.parseInt(mxBean.getName().replaceAll("@.*", ""));
    }

}