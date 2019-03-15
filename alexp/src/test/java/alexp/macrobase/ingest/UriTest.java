package alexp.macrobase.ingest;

import alexp.macrobase.utils.PathUtils;
import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class UriTest {
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void testUriParsing() {
        assertEquals(Uri.Type.CSV, new Uri("csv://documents/data.csv").getType());
        assertEquals("documents/data.csv", new Uri("csv://documents/data.csv").getPath());
        assertEquals("data.csv", new Uri("csv://documents/data.csv").shortDisplayPath());
        assertEquals("data", new Uri("csv://documents/data.csv").baseName());

        assertFalse(new Uri("csv://documents/data.csv").isDir());
        assertFalse(new Uri("csv://documents/data.dat").isDir());
        assertTrue(new Uri("csv://data_files/").isDir());
        assertTrue(new Uri("csv://data_files\\").isDir());
        assertEquals(Uri.Type.CSV, new Uri("csv://data_files/").getType());
        assertEquals("data_files/", new Uri("csv://data_files/").getPath());
        assertEquals("data_files", new Uri("csv://data_files/").baseName());
        assertEquals("data_files", new Uri("csv://data_files\\").baseName());

        assertEquals(Uri.Type.XLSX, new Uri("xls://data.xlsx").getType());
        assertEquals("data.xlsx", new Uri("xls://data.xlsx").getPath());
        assertEquals("data.xlsx", new Uri("xls://documents/data.xlsx").shortDisplayPath());
        assertEquals("data", new Uri("xls://documents/data.xlsx").baseName());

        assertEquals(Uri.Type.HTTP, new Uri("http://site.com/data").getType());
        assertEquals("http://site.com/data", new Uri("http://site.com/data").getPath());
        assertEquals("http", new Uri("http://site.com/data").getTypeString());
        assertEquals("http://site.com/data", new Uri("http://site.com/data").shortDisplayPath());

        assertEquals(Uri.Type.HTTP, new Uri("https://site.com/data").getType());
        assertEquals("https://site.com/data", new Uri("https://site.com/data").getPath());
        assertEquals("https", new Uri("https://site.com/data").getTypeString());

        assertEquals(Uri.Type.JDBC, new Uri("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword").getType());
        assertEquals("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword", new Uri("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword").getPath());
        assertEquals("", new Uri("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword").shortDisplayPath());
        assertEquals("", new Uri("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword").baseName());

        assertEquals(Uri.Type.UNKNOWN, new Uri("some/path").getType());
        assertEquals("some/path", new Uri("some/path").getPath());
        assertEquals("", new Uri("some/path").getTypeString());

        assertEquals(Uri.Type.UNKNOWN, new Uri("something://some/path").getType());
        assertEquals("something://some/path", new Uri("something://some/path").getPath());
        assertEquals("something", new Uri("something://some/path").getTypeString());
    }

    @Test
    public void testDirFiles() throws IOException {
        tmpFolder.newFile("1.csv");
        tmpFolder.newFile("2.csv");
        tmpFolder.newFile("3.csv");
        tmpFolder.newFile("results.json");
        tmpFolder.newFolder("subdir");
        tmpFolder.newFile("subdir/4.csv");

        String dirPath = tmpFolder.getRoot().getAbsolutePath() + File.separator;
        String url = "csv://" + dirPath;

        assertEquals(Lists.newArrayList("1.csv", "2.csv", "3.csv"), new Uri(url).getDirFiles(false));
    }

    @Test
    public void testDirFilesRecursive() throws IOException {
        tmpFolder.newFile("1.csv");
        tmpFolder.newFile("2.csv");
        tmpFolder.newFile("3.csv");
        tmpFolder.newFile("results.json");
        tmpFolder.newFolder("subdir", "subdir2", "subdir3");
        tmpFolder.newFile("subdir/4.csv");
        tmpFolder.newFile("subdir/subdir2/4.csv");

        String dirPath = tmpFolder.getRoot().getAbsolutePath() + File.separator;
        String url = "csv://" + dirPath;

        assertEquals(PathUtils.toNativeSeparators(Lists.newArrayList("1.csv", "2.csv", "3.csv", "subdir/4.csv", "subdir/subdir2/4.csv")), new Uri(url).getDirFiles(true));
    }

    @Test
    public void addsRootPath() {
        assertEquals("csv://~/data/documents/data.csv", new Uri("csv://documents/data.csv").addRootPath("~/data/").getOriginalString());
        assertEquals("csv://C:/data/documents/data.csv", new Uri("csv://documents/data.csv").addRootPath("C:/data").getOriginalString());
        assertEquals("csv://../data/documents/data.csv", new Uri("csv://documents/data.csv").addRootPath("../data/").getOriginalString());

        assertEquals("xls://~/data/documents/data.csv", new Uri("xls://documents/data.csv").addRootPath("~/data").getOriginalString());

        assertEquals("~/data/documents/data.csv", new Uri("documents/data.csv").addRootPath("~/data").getOriginalString());

        assertEquals("csv://documents/data.csv", new Uri("csv://documents/data.csv").addRootPath(null).getOriginalString());

        // no changes
        assertEquals("http://site.com/data", new Uri("http://site.com/data").addRootPath("~/data").getOriginalString());
        assertEquals("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword", new Uri("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword").addRootPath("~/data").getOriginalString());
    }

    @Test
    public void isImmutable() {
        Uri uri = new Uri("csv://documents/data.csv");
        Uri newUri = uri.addRootPath("~/data");

        assertNotEquals(newUri.getOriginalString(), uri.getOriginalString());
        assertEquals("csv://documents/data.csv", uri.getOriginalString());
        assertEquals("documents/data.csv", uri.getPath());
    }
}