package com.github.ksprojects;

import com.github.ksprojects.zkcopy.Node;
import com.github.ksprojects.zkcopy.reader.Reader;
import com.github.ksprojects.zkcopy.writer.Writer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class ZkCopy {

    private static final Logger LOGGER = Logger.getLogger(ZkCopy.class);
    private static final int DEFAULT_THREADS_NUMBER = 10;
    private static final int DEFAULT_ZK_SESSION_TIMEOUT = 3000;
    private static final boolean DEFAULT_REMOVE_DEPRECATED_NODES = true;
    private static final boolean DEFAULT_IGNORE_EPHEMERAL_NODES = true;
    private static final String HELP = "help";
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String WORKERS = "workers";
    private static final String SESSION_TIME_OUT = "sessionTimeout";
    private static final String COPY_ONLY = "copyOnly";
    private static final String IGNORE_EPHEMERAL_NODES = "ignoreEphemeralNodes";

    /**
     * Main entry point - start ZkCopy.
     */
    public static void main(String[] args) {
        Configuration cfg = parseLegacyConfiguration();
        if (cfg == null) {
            cfg = parseConfiguration(args);
        }
        if (cfg == null) {
            Options options = createOptions();
            printHelp(options);
            return;
        }
        String sourceAddress = cfg.source();
        String destinationAddress = cfg.target();
        int sessionTimeout = cfg.sessionTimeout();
        int threads = cfg.workers();
        boolean removeDeprecatedNodes = !cfg.copyOnly();
        LOGGER.info("using " + threads + " concurrent workers to copy data");
        LOGGER.info("delete nodes = " + String.valueOf(removeDeprecatedNodes));
        LOGGER.info("ignore ephemeral nodes = " + String.valueOf(cfg.ignoreEphemeralNodes()));
        Reader reader = new Reader(sourceAddress, threads);
        Node root = reader.read();
        if (root != null) {
            Writer writer = new Writer(destinationAddress, root, removeDeprecatedNodes, cfg.ignoreEphemeralNodes(), sessionTimeout);
            writer.write();
        } else {
            LOGGER.error("FAILED");
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("zkcopy", options);
    }

    private static Configuration parseConfiguration(String[] args) {
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption(HELP)) {
                printHelp(options);
                return null;
            }
            if (!line.hasOption(SOURCE) || !line.hasOption(TARGET)) {
                return null;
            }
            String sourceValue = getString(line, SOURCE);
            String targetValue = getString(line, TARGET);
            int workersValue = getInteger(line, WORKERS, DEFAULT_THREADS_NUMBER);
            int sessionTimeout = getInteger(line, SESSION_TIME_OUT, DEFAULT_ZK_SESSION_TIMEOUT);
            boolean copyOnlyValue = getBoolean(line, COPY_ONLY, !DEFAULT_REMOVE_DEPRECATED_NODES);
            boolean ignoreEphemeralNodes = getBoolean(line, IGNORE_EPHEMERAL_NODES, DEFAULT_IGNORE_EPHEMERAL_NODES);
            return ImmutableConfiguration.builder()
                    .source(sourceValue)
                    .target(targetValue)
                    .workers(workersValue)
                    .copyOnly(copyOnlyValue)
                    .ignoreEphemeralNodes(ignoreEphemeralNodes)
                    .sessionTimeout(sessionTimeout)
                    .build();
        } catch (ParseException exp) {
            LOGGER.error("Could not parse options.  Reason: " + exp.getMessage());
            return null;
        }
    }

    private static Options createOptions() {
        Options options = new Options();

        Option help = Option.builder("h")
                .longOpt(HELP)
                .desc("print this message")
                .build();
        Option source = Option.builder("s")
                .longOpt(SOURCE)
                .hasArg()
                .argName("server:port/path")
                .desc("location of a source tree to copy")
                .build();
        Option target = Option.builder("t")
                .longOpt(TARGET)
                .hasArg()
                .argName("server:port/path")
                .desc("target location")
                .build();
        Option workers = Option.builder("w")
                .longOpt(WORKERS)
                .hasArg()
                .argName("N")
                .desc("(optional) number of concurrent workers to copy data")
                .build();
        Option copyOnly = Option.builder("c")
                .longOpt(COPY_ONLY)
                .hasArg()
                .argName("true|false")
                .desc("(optional) set this flag if you do not want to remove nodes that are removed on source")
                .build();
        Option ignoreEphemeralNodes = Option.builder("i")
                .longOpt(IGNORE_EPHEMERAL_NODES)
                .hasArg()
                .argName("true|false")
                .desc("(optional) set this flag if you do not want to copy ephemeral ZNodes")
                .build();
        Option sessionTimeout = Option.builder("sto")
                .longOpt(SESSION_TIME_OUT)
                .hasArg()
                .argName("N")
                .desc("(optional) session timeout for zk connection in milli-seconds")
                .build();

        options.addOption(help);
        options.addOption(source);
        options.addOption(target);
        options.addOption(workers);
        options.addOption(copyOnly);
        options.addOption(ignoreEphemeralNodes);
        options.addOption(sessionTimeout);
        return options;
    }

    private static String getString(CommandLine line, String name) {
        return line.getOptionValue(name);
    }

    private static int getInteger(CommandLine line, String name, int defaultValue) {
        try {
            String value = line.getOptionValue(name);
            if (value == null) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("Could not parse option " + name + ": " + e.getMessage());
            return defaultValue;
        }
    }

    private static boolean getBoolean(CommandLine line, String name, boolean defaultValue) {
        try {
            String value = line.getOptionValue(name);
            if (value == null) {
                return defaultValue;
            }
            return Boolean.parseBoolean(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("Could not parse option " + name + ": " + e.getMessage());
            return defaultValue;
        }
    }

    private static Configuration parseLegacyConfiguration() {
        String sourceAddress = getSource();
        String destinationAddress = getDestination();
        int sessionTimeout = getSessionTimeout();
        if (sourceAddress == null || destinationAddress == null) {
            return null;
        }
        int threads = getThreadsNumber();
        boolean removeDeprecatedNodes = getRemoveDeprecatedNodes();
        return ImmutableConfiguration.builder()
                .source(sourceAddress)
                .target(destinationAddress)
                .workers(threads)
                .sessionTimeout(sessionTimeout)
                .copyOnly(!removeDeprecatedNodes)
                .build();
    }

    private static String getDestination() {
        return System.getProperty("destination");
    }

    private static String getSource() {
        return System.getProperty("source");
    }

    private static int getSessionTimeout() {
        String sessionTimeout = System.getProperty("sessionTimeout");
        int n = DEFAULT_ZK_SESSION_TIMEOUT;
        if (sessionTimeout == null) {
            return DEFAULT_ZK_SESSION_TIMEOUT;
        }
        try {
            n = Integer.valueOf(sessionTimeout);
        } catch (NumberFormatException e) {
            LOGGER.error("Can't parse threads number - \"" + sessionTimeout + "\"", e);
        }
        return n;
    }

    private static int getThreadsNumber() {
        String threads = System.getProperty("threads");
        int n = DEFAULT_THREADS_NUMBER;
        if (threads == null) {
            return DEFAULT_THREADS_NUMBER;
        }
        try {
            n = Integer.valueOf(threads);
        } catch (NumberFormatException e) {
            LOGGER.error("Can't parse threads number - \"" + threads + "\"", e);
        }
        return n;
    }

    private static boolean getRemoveDeprecatedNodes() {
        String s = System.getProperty("removeDeprecatedNodes");
        boolean ans = DEFAULT_REMOVE_DEPRECATED_NODES;
        if (s == null) {
            return DEFAULT_REMOVE_DEPRECATED_NODES;
        }
        try {
            ans = Boolean.valueOf(s);
        } catch (NumberFormatException e) {
            LOGGER.error("Can't parse 'removeDeprecatedNodes' - \"" + s + "\"", e);
        }
        return ans;
    }

}


