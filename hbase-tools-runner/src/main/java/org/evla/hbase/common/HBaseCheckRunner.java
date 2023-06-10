package org.evla.hbase.common;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.Pair;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.StringJoiner;

public class HBaseCheckRunner implements HBaseToolRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseCheckRunner.class);

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length != 1) {
            throw new RuntimeException();
        }
        try {
            Map<RegionInfo, Pair<Result, Result>> rows = new HBaseRegionChecker(TableName.valueOf(args[0])).checkTable(admin);
            int count = 1;
            StringJoiner sj = new StringJoiner("\n");
            for (Map.Entry<RegionInfo, Pair<Result, Result>> entry : rows.entrySet()) {
                StringBuilder sb = new StringBuilder();
                if (entry.getValue() == null) {
                    throw new RuntimeException("Data in table " + args[0] + " is not available");
                }
                Result r1 = entry.getValue().getFirst();
                Result r2 = entry.getValue().getSecond();

                Cell c1 = null;
                if (r1 != null && r1.advance()) {
                    c1 = r1.current();
                }
                Cell c2 = null;
                if (r2 != null && r2.advance()) {
                    c2 = r2.current();
                }

                String str1 = c1 == null ? "" : Bytes.toStringBinary(CellUtil.cloneRow(c1));
                String str2 = c2 == null ? "" : Bytes.toStringBinary(CellUtil.cloneRow(c2));

                sb.append("Region #")
                        .append(count)
                        .append(" [")
                        .append(entry.getKey().getEncodedName())
                        .append("]: (");
                if (c1 != null && c2 != null) {
                    sb.append(str1).append(";").append(str2);
                } else {
                    sb.append("region is empty");
                }
                sb.append(")");

                sj.add(sb.toString());
                count++;
            }
            LOGGER.info("Table {} checked:\n{}", args[0], sj);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

    }
}