package alexp.macrobase.utils;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class PathUtils {
    public static String toNativeSeparators(String path) {
        return path.replace('/', File.separatorChar);
    }

    public static List<String> toNativeSeparators(List<String> paths) {
        return paths.stream()
                .map(PathUtils::toNativeSeparators)
                .collect(Collectors.toList());
    }
}
