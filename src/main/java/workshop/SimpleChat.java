package workshop;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * JGroups tutorial chat extended with a replicated HashMap&lt;String,String&gt; and a write-ahead log.
 *
 * Usage: java workshop.SimpleChat &lt;nodeName&gt;     (e.g. Bogota, London, Virginia)
 *
 * Commands: /store &lt;key&gt; &lt;value&gt;, /state, /quit — anything else is a chat message.
 */
public class SimpleChat implements Receiver {
    private static final String CLUSTER = "ChatCluster";

    private final String nodeName;
    private JChannel channel;
    private View lastView;

    // ---- everything below is guarded by "this" ----
    private final List<String> history = new LinkedList<>();          // chat history (tutorial)
    private final Map<String, String> map = new HashMap<>();          // replicated key-value store
    private final Set<String> appliedIds = new HashSet<>();           // ids of operations already applied
    private final List<StoreCommand> appliedOps = new ArrayList<>();  // applied operations, in order
    private final WriteAheadLog wal;

    public SimpleChat(String nodeName) throws Exception {
        this.nodeName = nodeName;
        Path walDir = Paths.get(System.getProperty("wal.dir", "."));
        this.wal = new WriteAheadLog(walDir.resolve(nodeName + ".wal"));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: SimpleChat <nodeName>");
            return;
        }
        new SimpleChat(args[0]).start();
    }

    private void start() throws Exception {
        // 1. Recovery: rebuild the map from the local WAL BEFORE joining the cluster.
        replayWal();

        // 2. Join the cluster.
        String config = System.getProperty("jgroups.config", "tcp-local.xml");
        channel = new JChannel(config).name(nodeName).setReceiver(this);
        channel.connect(CLUSTER);

        // 3. State transfer: ask the coordinator for chat history + applied operations.
        //    Returns immediately if we are the first (only) member.
        channel.getState(null, 10000);

        eventLoop();
        channel.close();
        wal.close();
    }

    // ------------------------------------------------------------------ WAL replay

    private void replayWal() throws Exception {
        List<StoreCommand> ops = wal.readAll();
        System.out.println("[WAL] replaying " + ops.size() + " operation(s) from " + wal.path());
        synchronized (this) {
            for (StoreCommand cmd : ops) {
                if (appliedIds.add(cmd.id())) {        // ignore duplicated lines
                    map.put(cmd.key(), cmd.value());
                    appliedOps.add(cmd);
                    System.out.println("[WAL]   replay " + cmd.key() + "=" + cmd.value() + "  (" + cmd.id() + ")");
                }
            }
            System.out.println("[WAL] map after replay: " + new TreeMap<>(map));
        }
    }

    // ------------------------------------------------------------------ apply (WAL first, then memory)

    /** Applies a replicated operation exactly once: append + fsync to the WAL, then update the HashMap. */
    private synchronized boolean apply(StoreCommand cmd, String source) throws Exception {
        if (appliedIds.contains(cmd.id())) {
            System.out.println("[store] duplicate ignored: " + cmd.id() + " (from " + source + ")");
            return false;
        }
        wal.append(cmd);                 // 1-2. append to log file and force it to disk
        map.put(cmd.key(), cmd.value());     // 3.   apply to memory
        appliedIds.add(cmd.id());
        appliedOps.add(cmd);
        return true;
    }

    // ------------------------------------------------------------------ JGroups callbacks

    @Override
    public void viewAccepted(View newView) {
        System.out.println("** view: " + newView);
        if (lastView != null) {
            List<Address> joined = View.newMembers(lastView, newView);
            List<Address> left = View.leftMembers(lastView, newView);
            if (!joined.isEmpty()) System.out.println("** joined: " + joined);
            if (!left.isEmpty()) System.out.println("** left/crashed: " + left);
        }
        lastView = newView;
    }

    @Override
    public void receive(Message msg) {
        Object payload = msg.getObject();
        String src = String.valueOf(msg.getSrc());
        try {
            if (payload instanceof StoreCommand) {
                StoreCommand cmd = (StoreCommand) payload;
                if (apply(cmd, src))
                    System.out.println("[store] " + src + " stored " + cmd.key() + "=" + cmd.value() + "  (" + cmd.id() + ")");
            } else {
                String line = src + ": " + payload;
                System.out.println(line);
                synchronized (this) {
                    history.add(line);
                }
            }
        } catch (Exception e) {
            System.err.println("failed to handle message from " + src + ": " + e);
        }
    }

    /** Called on the coordinator: send chat history + every applied operation (in order). */
    @Override
    public void getState(OutputStream output) throws Exception {
        synchronized (this) {
            DataOutputStream out = new DataOutputStream(output);
            Util.objectToStream(new ArrayList<>(history), out);
            Util.objectToStream(new ArrayList<>(appliedOps), out);
            System.out.println("[state-transfer] sent " + history.size() + " chat line(s) and "
                    + appliedOps.size() + " operation(s)");
        }
    }

    /** Called on a joining node: replace history, apply (through the WAL) the operations we are missing. */
    @Override
    @SuppressWarnings("unchecked")
    public void setState(InputStream input) throws Exception {
        DataInputStream in = new DataInputStream(input);
        List<String> remoteHistory = Util.objectFromStream(in);
        List<StoreCommand> remoteOps = Util.objectFromStream(in);
        int missing = 0;
        synchronized (this) {
            history.clear();
            history.addAll(remoteHistory);
            for (StoreCommand cmd : remoteOps) {
                if (!appliedIds.contains(cmd.id())) {
                    apply(cmd, "state-transfer");
                    missing++;
                }
            }
            System.out.println("[state-transfer] received " + remoteHistory.size() + " chat line(s), "
                    + remoteOps.size() + " operation(s), " + missing + " were new for this node");
            remoteHistory.forEach(System.out::println);
            System.out.println("[state-transfer] map now: " + new TreeMap<>(map));
        }
    }

    // ------------------------------------------------------------------ console

    private void eventLoop() {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                System.out.print("> ");
                System.out.flush();
                String line = in.readLine();
                if (line == null || line.equalsIgnoreCase("/quit") || line.equalsIgnoreCase("quit"))
                    break;
                line = line.trim();
                if (line.isEmpty())
                    continue;

                if (line.equals("/store") || line.startsWith("/store ")) {
                    String[] parts = line.split("\\s+", 3);
                    if (parts.length < 3) {
                        System.out.println("usage: /store <key> <value>");
                        continue;
                    }
                    StoreCommand cmd = StoreCommand.create(nodeName, parts[1], parts[2]);
                    channel.send(new ObjectMessage(null, cmd));   // null destination = whole group (we receive it too)
                } else if (line.equals("/state")) {
                    printState();
                } else {
                    channel.send(new ObjectMessage(null, line));            // plain chat message
                }
            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }
    }

    private synchronized void printState() {
        System.out.println("[state] " + nodeName + " view=" + channel.getView());
        System.out.println("[state] " + map.size() + " key(s), " + appliedOps.size() + " operation(s) applied");
        new TreeMap<>(map).forEach((k, v) -> System.out.println("[state]   " + k + " = " + v));
    }
}
