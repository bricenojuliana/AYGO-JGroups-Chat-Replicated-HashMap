package workshop;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only write-ahead log, one text line per operation:
 *
 *     STORE&lt;TAB&gt;opId&lt;TAB&gt;key&lt;TAB&gt;value
 *
 * Tabs and newlines inside key/value are escaped so one line is always one operation.
 */
public class WriteAheadLog implements AutoCloseable {
    private final Path path;
    private final FileOutputStream out;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path;
        dropTornTail();
        this.out = new FileOutputStream(path.toFile(), true); // true = append
    }

    /**
     * A crash in the middle of append() can leave a last line without its '\n'. That record was never
     * acknowledged, so cut it off: it is not replayed and the next record starts on a line of its own.
     */
    private void dropTornTail() throws IOException {
        if (!Files.exists(path)) return;
        byte[] data = Files.readAllBytes(path);
        int end = data.length;
        while (end > 0 && data[end - 1] != '\n') end--;
        if (end < data.length) {
            System.out.println("[WAL] dropping torn last record: "
                    + new String(data, end, data.length - end, StandardCharsets.UTF_8));
            try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
                ch.truncate(end);
                ch.force(true);
            }
        }
    }

    public Path path() {
        return path;
    }

    /** Appends the operation and forces it to disk before returning. */
    public synchronized void append(StoreCommand cmd) throws IOException {
        String line = "STORE\t" + escape(cmd.id()) + "\t" + escape(cmd.key()) + "\t" + escape(cmd.value()) + "\n";
        out.write(line.getBytes(StandardCharsets.UTF_8));
        out.flush();
        out.getFD().sync(); // fsync: the record is on persistent storage now
    }

    /** Reads every well-formed record of the log (the torn tail, if any, was already dropped). */
    public List<StoreCommand> readAll() throws IOException {
        List<StoreCommand> ops = new ArrayList<>();
        if (!Files.exists(path)) return ops;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String[] f = line.split("\t", -1);
            if (f.length != 4 || !f[0].equals("STORE")) {
                System.out.println("[WAL] skipping malformed line: " + line);
                continue;
            }
            ops.add(new StoreCommand(unescape(f[1]), unescape(f[2]), unescape(f[3])));
        }
        return ops;
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n");
    }

    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                sb.append(n == 't' ? '\t' : n == 'n' ? '\n' : n);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
