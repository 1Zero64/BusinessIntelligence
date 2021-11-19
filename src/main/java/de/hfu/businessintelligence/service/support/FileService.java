package de.hfu.businessintelligence.service.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.stream.Collectors;

import static de.hfu.businessintelligence.configuration.CsvConfiguration.RESULT_DIRECTORY;
import static de.hfu.businessintelligence.configuration.CsvConfiguration.WITH_HEADER;

@Slf4j
public class FileService {

    private static final FileService instance = new FileService();

    private FileService() {

    }

    public static FileService getInstance() {
        return instance;
    }

    public String[] getAllFilePathsFrom(String directory) {
        LinkedHashSet<String> filePaths = new LinkedHashSet<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(Paths.get(directory))) {
            for (Path path : paths) {
                if (!Files.isDirectory(path)) {
                    log.info("Reading file path: ".concat(path.toFile().getAbsolutePath()));
                    filePaths.add(path.toFile().getAbsolutePath());
                }
            }
        } catch (IOException e) {
            log.error("Error while reading file name in directory: ", e);
        }
        return filePaths.stream()
                .sorted()
                .collect(Collectors.toList())
                .toArray(new String[]{});
    }

    public void saveAsCsvFile(Dataset<Row> table, String tableName) {
        table.write()
                .option("header", WITH_HEADER)
                .mode(SaveMode.Overwrite)
                .csv(RESULT_DIRECTORY.concat(tableName));
    }
}
