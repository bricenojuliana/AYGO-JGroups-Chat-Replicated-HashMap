package workshop;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteAheadLogTest {
    @TempDir
    Path dir;

    @Test
    void missingFileReplaysNothing() throws Exception {
        try (WriteAheadLog wal = new WriteAheadLog(dir.resolve("Node.wal"))) {
            assertTrue(wal.readAll().isEmpty());
        }
    }

    @Test
    void recordsSurviveReopenInOrder() throws Exception {
        Path file = dir.resolve("Node.wal");
        StoreCommand a = new StoreCommand("id1", "color", "azul");
        StoreCommand b = new StoreCommand("id2", "color", "verde");
        try (WriteAheadLog wal = new WriteAheadLog(file)) {
            wal.append(a);
            wal.append(b);
        }
        try (WriteAheadLog wal = new WriteAheadLog(file)) {
            assertEquals(List.of(a, b), wal.readAll());
        }
    }

    @Test
    void tabsNewlinesAndBackslashesRoundTrip() throws Exception {
        Path file = dir.resolve("Node.wal");
        StoreCommand cmd = new StoreCommand("id1", "k\tey", "line1\nline2 \\t not a tab \\ ñ");
        try (WriteAheadLog wal = new WriteAheadLog(file)) {
            wal.append(cmd);
        }
        assertEquals(1, Files.readAllLines(file).size());   // still one line per operation
        try (WriteAheadLog wal = new WriteAheadLog(file)) {
            assertEquals(List.of(cmd), wal.readAll());
        }
    }

    @Test
    void tornLastRecordIsDroppedAndNextRecordStartsOnItsOwnLine() throws Exception {
        Path file = dir.resolve("Node.wal");
        Files.writeString(file, "STORE\tid1\tk\tv\nSTORE\tid2\tk2\tvalu", StandardCharsets.UTF_8);
        StoreCommand next = new StoreCommand("id3", "k3", "v3");
        try (WriteAheadLog wal = new WriteAheadLog(file)) {
            assertEquals(List.of(new StoreCommand("id1", "k", "v")), wal.readAll());
            wal.append(next);
        }
        try (WriteAheadLog wal = new WriteAheadLog(file)) {
            assertEquals(List.of(new StoreCommand("id1", "k", "v"), next), wal.readAll());
        }
    }

    @Test
    void malformedLinesAreSkipped() throws Exception {
        Path file = dir.resolve("Node.wal");
        Files.writeString(file, "garbage\nSTORE\tid1\tk\tv\nPUT\tid2\tk\tv\n", StandardCharsets.UTF_8);
        try (WriteAheadLog wal = new WriteAheadLog(file)) {
            assertEquals(List.of(new StoreCommand("id1", "k", "v")), wal.readAll());
        }
    }

    @Test
    void createdCommandsHaveUniqueIdsPrefixedByOrigin() {
        StoreCommand a = StoreCommand.create("London", "k", "v");
        StoreCommand b = StoreCommand.create("London", "k", "v");
        assertTrue(a.id().startsWith("London-"));
        assertNotEquals(a.id(), b.id());
    }
}
