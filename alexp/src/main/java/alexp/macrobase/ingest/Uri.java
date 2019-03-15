package alexp.macrobase.ingest;

import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class Uri {
    public enum Type {
        UNKNOWN,
        CSV,
        XLSX,
        HTTP,
        JDBC
    }

    private final String originalString;

    private final String path;
    private final String typeString;
    private final Type type;

    public Uri(String originalString) {
        this.originalString = originalString;

        if (!originalString.contains(":")) {
            type = Type.UNKNOWN;
            path = originalString;
            typeString = "";
        } else {
            typeString = originalString.substring(0, originalString.indexOf(":")).toLowerCase();

            switch (typeString.toLowerCase()) {
                case "csv":
                    type = Type.CSV;
                    path = originalString.substring(typeString.length() + 1).replace("//", "");
                    break;
                case "xls":
                    type = Type.XLSX;
                    path = originalString.substring(typeString.length() + 1).replace("//", "");
                    break;
                case "http":
                case "https":
                    type = Type.HTTP;
                    path = originalString;
                    break;
                case "jdbc":
                    type = Type.JDBC;
                    path = originalString;
                    break;
                default:
                    type = Type.UNKNOWN;
                    path = originalString;
                    break;
            }
        }
    }

    public String getOriginalString() {
        return originalString;
    }

    public String getPath() {
        return path;
    }

    public String getTypeString() {
        return typeString;
    }

    public Type getType() {
        return type;
    }

    public boolean isDir() {
        return path.endsWith("/") || path.endsWith("\\");
    }

    public String shortDisplayPath() {
        switch (type) {
            case CSV:
            case XLSX:
                return new File(getPath()).getName();
            case HTTP:
                return getOriginalString();
            case JDBC:
                return "";
            default:
                return getPath();
        }
    }

    public String baseName() {
        if (isDir()) {
            return FilenameUtils.getBaseName(path.substring(0, path.length() - 1));
        }
        switch (type) {
            case CSV:
            case XLSX:
                return FilenameUtils.getBaseName(path);
            default:
                return "";
        }
    }

    public List<String> getDirFiles(boolean recursive, List<String> extensions) throws IOException {
        if (!isDir()) {
            throw new IllegalStateException(path + "is not a dir");
        }

        switch (type) {
            case CSV:
            case XLSX:
                break;
            default:
                throw new IllegalStateException("Cannot load files for type " + type);
        }

        Path root = Paths.get(path);

        return Files.walk(root, recursive ? 255 : 1)
                .filter(p -> Files.isRegularFile(p) && (extensions == null || extensions.isEmpty() || extensions.stream().anyMatch(ext -> p.toString().endsWith(ext))))
                .map(p -> root.relativize(p).toString())
                .sorted()
                .collect(toList());
    }

    public List<String> getDirFiles(boolean recursive) throws IOException {
        switch (type) {
            case CSV:
                return getDirFiles(recursive, Lists.newArrayList(".csv"));
            case XLSX:
                return getDirFiles(recursive, Lists.newArrayList(".xlsx", ".xlsm"));
            default:
                return getDirFiles(recursive, new ArrayList<>());
        }
    }

    public Uri addRootPath(String rootPath) {
        if (rootPath == null)
            return this;

        if (!rootPath.endsWith("/"))
            rootPath = rootPath + "/";
        
        switch (type) {
            case CSV:
                return new Uri("csv://" + rootPath + path);
            case XLSX:
                return new Uri("xls://" + rootPath + path);
            case UNKNOWN:
                return new Uri(rootPath + path);
            default:
                return this;
        }
    }
}
