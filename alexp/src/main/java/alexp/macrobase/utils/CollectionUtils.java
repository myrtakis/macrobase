package alexp.macrobase.utils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public class CollectionUtils {
    public static <T> List<List<T>> transpose(List<List<T>> table) {
        List<List<T>> ret = new ArrayList<>();
        final int N = table.get(0).size();
        for (int i = 0; i < N; i++) {
            List<T> col = new ArrayList<>();
            for (List<T> row : table) {
                col.add(row.get(i));
            }
            ret.add(col);
        }
        return ret;
    }

    public static <T> List<T> listOrSingleNullElement(List<T> list) {
        return (list == null || list.isEmpty()) ? Lists.newArrayList((T) null) : list;
    }
}
