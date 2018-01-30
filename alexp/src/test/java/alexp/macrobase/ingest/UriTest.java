package alexp.macrobase.ingest;

import org.junit.Test;

import static org.junit.Assert.*;

public class UriTest {

    @Test
    public void testUriParsing() {
        assertEquals(Uri.Type.CSV, new Uri("csv://documents/data.csv").getType());
        assertEquals("documents/data.csv", new Uri("csv://documents/data.csv").getPath());

        assertEquals(Uri.Type.XLSX, new Uri("xls://data.xlsx").getType());
        assertEquals("data.xlsx", new Uri("xls://data.xlsx").getPath());

        assertEquals(Uri.Type.HTTP, new Uri("http://site.com/data").getType());
        assertEquals("http://site.com/data", new Uri("http://site.com/data").getPath());
        assertEquals("http", new Uri("http://site.com/data").getTypeString());

        assertEquals(Uri.Type.HTTP, new Uri("https://site.com/data").getType());
        assertEquals("https://site.com/data", new Uri("https://site.com/data").getPath());
        assertEquals("https", new Uri("https://site.com/data").getTypeString());

        assertEquals(Uri.Type.JDBC, new Uri("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword").getType());
        assertEquals("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword", new Uri("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=pgpassword").getPath());

        assertEquals(Uri.Type.UNKNOWN, new Uri("some/path").getType());
        assertEquals("some/path", new Uri("some/path").getPath());
        assertEquals("", new Uri("some/path").getTypeString());

        assertEquals(Uri.Type.UNKNOWN, new Uri("something://some/path").getType());
        assertEquals("something://some/path", new Uri("something://some/path").getPath());
        assertEquals("something", new Uri("something://some/path").getTypeString());
    }
}