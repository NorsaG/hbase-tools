package org.evla.hbase;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ToolsRunner {
    private static final String PROPS_FILE = "./hbase-tools.properties";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printHelp();
            throw new IllegalArgumentException("HBase tool is not specified");
        }
        Properties properties = readProperties();
        String strTool = args[0].toUpperCase();
        String[] toolArguments = null;
        if (args.length > 1) {
            toolArguments = new String[args.length - 1];
            System.arraycopy(args, 1, toolArguments, 0, args.length - 1);
        }

        if ("help".equalsIgnoreCase(strTool)) {
            String name = args.length >= 2 ? args[1] : null;
            if (name == null) {
                printFullHelp();
            } else {
                printToolDescription(Tool.valueOf(name.toUpperCase()));
            }
        } else {
            Tool tool = Tool.valueOf(strTool);
            if (HBaseToolsSettings.customizeLogging(properties)) {
                LogConfigurator.configureLogging(tool);
            }
            HBaseToolsSettings settings = new HBaseToolsSettings(properties);
            StaticConnector.configure(settings);

            Admin admin = StaticConnector.getAdmin();
            tool.run(admin, settings, toolArguments);
        }
    }

    private static void printHelp() {
        System.out.println("Not correct usage of hbase-tools. Print \"help\" for more information.");
    }

    private static void printFullHelp() {
        System.out.println("List of available tools:");
        for (Tool t : Tool.values()) {
            System.out.println("\t" + t.getToolDescription());
        }
    }

    private static void printToolDescription(Tool tool) {
        System.out.println("-----------------------------------------------------------------------");
        System.out.println(tool.getToolDescription());
        System.out.println(tool.getToolUsageString());
        System.out.println("-----------------------------------------------------------------------");
    }

    private static Properties readProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(PROPS_FILE));
        return properties;
    }

}
