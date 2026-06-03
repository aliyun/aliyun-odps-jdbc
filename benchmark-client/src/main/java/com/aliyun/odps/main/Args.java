package com.aliyun.odps.main;

import lombok.Getter;
import lombok.ToString;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

@ToString
@Getter
public class Args {
    private String configFilePath = "config.ini";
    private String globalSettingFile = "";
    private String indexFilePath;
    //private String outputDir = "output";

    public void parse(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("c", "config", true, "config file path");
        options.addOption("s", "setting", true, "global setting file path");
        options.addOption("i", "index", true, "index file path");

        CommandLineParser parser = new DefaultParser();
        CommandLine cli = parser.parse(options, args);

        if (cli.hasOption("c")) {
            this.configFilePath = cli.getOptionValue("c");
        }

        if (cli.hasOption("s")) {
            globalSettingFile = cli.getOptionValue("s");
        }

        if (cli.hasOption("i")) {
            this.indexFilePath = cli.getOptionValue("i");
        } else {
            throw new ParseException("index file path is required");
        }
    }

}
