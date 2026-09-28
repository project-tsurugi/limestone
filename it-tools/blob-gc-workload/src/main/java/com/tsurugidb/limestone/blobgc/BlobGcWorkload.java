package com.tsurugidb.limestone.blobgc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.common.BlobTransferType;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;

/**
 * A workload to exercise the garbage collection of BLOB files at online compaction.
 *
 * It repeats INSERT / UPDATE / DELETE on a table with a BLOB column to produce both live
 * BLOBs and garbage ({@code churn}), and then reads every row back and verifies the BLOB
 * contents ({@code verify}). The content of a BLOB is derived from (k, gen, sz), so the
 * verification needs no state outside the database.
 *
 * <pre>
 * blob-gc-workload create [--url URL]
 * blob-gc-workload churn  [--url URL] [--rows N] [--iterations N] [--size BYTES]
 *                         [--tmpdir DIR] [--hold-ms MS] [--hold-every N]
 * blob-gc-workload verify [--url URL]
 * blob-gc-workload count  [--url URL]
 * </pre>
 */
public final class BlobGcWorkload {

    private static final String TABLE = "blob_gc_test";

    private String url = "ipc:tsurugi";
    private int rows = 300;
    private int iterations = 20;
    private int size = 16 * 1024;
    private Path tmpdir = Paths.get("/tmp/blob-gc-workload");
    private long holdMs = 1500;
    private int holdEvery = 25;

    private enum Op { INSERT, UPDATE, DELETE }

    public static void main(String[] args) {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        BlobGcWorkload w = new BlobGcWorkload();
        String command = args[0];
        try {
            w.parseOptions(args);
            switch (command) {
            case "create":
                w.create();
                break;
            case "churn":
                w.churn();
                break;
            case "verify":
                System.exit(w.verify() ? 0 : 1);
                break;
            case "count":
                w.count();
                break;
            default:
                usage();
                System.exit(2);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(2);
        }
    }

    private static void usage() {
        System.err.println("usage: blob-gc-workload <create|churn|verify|count> [--url URL] [--rows N]"
                + " [--iterations N] [--size BYTES] [--tmpdir DIR] [--hold-ms MS] [--hold-every N]");
    }

    private void parseOptions(String[] args) {
        for (int i = 1; i < args.length; i += 2) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for " + args[i]);
            }
            String key = args[i];
            String value = args[i + 1];
            switch (key) {
            case "--url":
                url = value;
                break;
            case "--rows":
                rows = Integer.parseInt(value);
                break;
            case "--iterations":
                iterations = Integer.parseInt(value);
                break;
            case "--size":
                size = Integer.parseInt(value);
                break;
            case "--tmpdir":
                tmpdir = Paths.get(value);
                break;
            case "--hold-ms":
                holdMs = Long.parseLong(value);
                break;
            case "--hold-every":
                holdEvery = Integer.parseInt(value);
                break;
            default:
                throw new IllegalArgumentException("unknown option: " + key);
            }
        }
    }

    /**
     * Opens a session. BLOBs are transferred in the privileged mode (by file path).
     * The connection times out after 10 seconds so that a wrong URL fails instead of
     * waiting for the server forever.
     *
     * The default of tsubakuro (BlobTransferType.DEFAULT) offers RELAY and then DOES_NOT_USE,
     * so the session cannot handle BLOBs when the BLOB relay service (gRPC) of the server is
     * disabled. The privileged mode works as long as allow_blob_privileged of the IPC
     * endpoint (true by default) permits it.
     */
    private Session connect() throws Exception {
        return SessionBuilder.connect(url).withBlobTransfer(BlobTransferType.PRIVILEGED)
                .create(10, TimeUnit.SECONDS);
    }

    /** Recreates the table. */
    private void create() throws Exception {
        try (Session session = connect();
                SqlClient client = SqlClient.attach(session)) {
            try (Transaction tx = client.createTransaction().await()) {
                tx.executeStatement("DROP TABLE IF EXISTS " + TABLE).await();
                tx.commit().await();
            }
            try (Transaction tx = client.createTransaction().await()) {
                tx.executeStatement("CREATE TABLE " + TABLE
                        + " (k INT PRIMARY KEY, gen INT NOT NULL, sz INT NOT NULL, b BLOB NOT NULL)").await();
                tx.commit().await();
            }
            System.out.println("table " + TABLE + " created");
        }
    }

    /**
     * Repeats INSERT / UPDATE / DELETE, one statement per transaction.
     *
     * In each iteration, a row that does not exist is inserted, a row with k % 7 == 0 is
     * deleted, and a row with k % 3 == 0 is updated (the old BLOB of an updated or deleted
     * row becomes garbage). Once in hold-every transactions, the statement that registered
     * a BLOB waits hold-ms before the commit, which widens the time in which the BLOB is
     * registered but not committed yet.
     */
    private void churn() throws Exception {
        Files.createDirectories(tmpdir);
        Set<Integer> present = new HashSet<>();
        int txCount = 0;
        int blobsCreated = 0;
        try (Session session = connect();
                SqlClient client = SqlClient.attach(session);
                PreparedStatement insert = client.prepare(
                        "INSERT INTO " + TABLE + " (k, gen, sz, b) VALUES (:k, :gen, :sz, :b)",
                        Placeholders.of("k", int.class),
                        Placeholders.of("gen", int.class),
                        Placeholders.of("sz", int.class),
                        Placeholders.of("b", SqlCommon.Blob.class)).await();
                PreparedStatement update = client.prepare(
                        "UPDATE " + TABLE + " SET gen = :gen, sz = :sz, b = :b WHERE k = :k",
                        Placeholders.of("k", int.class),
                        Placeholders.of("gen", int.class),
                        Placeholders.of("sz", int.class),
                        Placeholders.of("b", SqlCommon.Blob.class)).await();
                PreparedStatement delete = client.prepare(
                        "DELETE FROM " + TABLE + " WHERE k = :k",
                        Placeholders.of("k", int.class)).await()) {
            for (int it = 1; it <= iterations; it++) {
                int inserted = 0;
                int updated = 0;
                int deleted = 0;
                int held = 0;
                for (int k = 0; k < rows; k++) {
                    Op op;
                    if (!present.contains(k)) {
                        op = Op.INSERT;
                    } else if (k % 7 == 0) {
                        op = Op.DELETE;
                    } else if (k % 3 == 0) {
                        op = Op.UPDATE;
                    } else {
                        continue;
                    }
                    txCount++;
                    Path file = null;
                    try (Transaction tx = client.createTransaction().await()) {
                        switch (op) {
                        case INSERT:
                            file = writeBlobFile(k, it);
                            tx.executeStatement(insert,
                                    Parameters.of("k", k), Parameters.of("gen", it),
                                    Parameters.of("sz", size), Parameters.blobOf("b", file)).await();
                            break;
                        case UPDATE:
                            file = writeBlobFile(k, it);
                            tx.executeStatement(update,
                                    Parameters.of("k", k), Parameters.of("gen", it),
                                    Parameters.of("sz", size), Parameters.blobOf("b", file)).await();
                            break;
                        case DELETE:
                            tx.executeStatement(delete, Parameters.of("k", k)).await();
                            break;
                        default:
                            throw new IllegalStateException();
                        }
                        if (file != null && holdMs > 0 && holdEvery > 0 && txCount % holdEvery == 0) {
                            Thread.sleep(holdMs);
                            held++;
                        }
                        tx.commit().await();
                        if (file != null) {
                            blobsCreated++;
                        }
                    } finally {
                        if (file != null) {
                            Files.deleteIfExists(file);
                        }
                    }
                    switch (op) {
                    case INSERT:
                        present.add(k);
                        inserted++;
                        break;
                    case UPDATE:
                        updated++;
                        break;
                    case DELETE:
                        present.remove(k);
                        deleted++;
                        break;
                    default:
                        throw new IllegalStateException();
                    }
                }
                System.out.printf("iteration %d: inserted=%d updated=%d deleted=%d held=%d rows=%d%n",
                        it, inserted, updated, deleted, held, present.size());
            }
        }
        System.out.printf("churn finished: transactions=%d blobs_created=%d rows=%d%n",
                txCount, blobsCreated, present.size());
    }

    /**
     * Reads every row back and compares its BLOB with the content derived from (k, gen, sz).
     *
     * @return true if every row matches
     */
    private boolean verify() throws Exception {
        int total = 0;
        int ok = 0;
        int bad = 0;
        try (Session session = connect();
                SqlClient client = SqlClient.attach(session);
                Transaction tx = client.createTransaction().await();
                ResultSet rs = tx.executeQuery("SELECT k, gen, sz, b FROM " + TABLE + " ORDER BY k").await()) {
            while (rs.nextRow()) {
                rs.nextColumn();
                int k = rs.fetchInt4Value();
                rs.nextColumn();
                int gen = rs.fetchInt4Value();
                rs.nextColumn();
                int sz = rs.fetchInt4Value();
                rs.nextColumn();
                var ref = rs.fetchBlob();
                total++;
                try (InputStream in = tx.openInputStream(ref).await()) {
                    byte[] actual = in.readAllBytes();
                    if (Arrays.equals(actual, expected(k, gen, sz))) {
                        ok++;
                    } else {
                        bad++;
                        System.out.printf("MISMATCH k=%d gen=%d: expected %d bytes, got %d bytes%n",
                                k, gen, sz, actual.length);
                    }
                } catch (Exception e) {
                    bad++;
                    System.out.printf("ERROR k=%d gen=%d: %s%n", k, gen, e);
                }
            }
            tx.commit().await();
        }
        System.out.printf("verify finished: rows=%d ok=%d bad=%d%n", total, ok, bad);
        return bad == 0;
    }

    /** Prints the number of rows. */
    private void count() throws Exception {
        try (Session session = connect();
                SqlClient client = SqlClient.attach(session);
                Transaction tx = client.createTransaction().await();
                ResultSet rs = tx.executeQuery("SELECT COUNT(*) FROM " + TABLE).await()) {
            if (rs.nextRow() && rs.nextColumn()) {
                System.out.println("rows=" + rs.fetchInt8Value());
            }
            tx.commit().await();
        }
    }

    /** Writes the BLOB content of (k, gen) to a temporary file and returns its path. */
    private Path writeBlobFile(int k, int gen) throws IOException {
        Path file = tmpdir.resolve("blob_" + k + "_" + gen + ".bin");
        Files.write(file, expected(k, gen, size));
        return file;
    }

    /** Derives the BLOB content from (k, gen, sz). */
    private static byte[] expected(int k, int gen, int sz) {
        byte[] data = new byte[sz];
        for (int i = 0; i < sz; i++) {
            data[i] = (byte) (k * 31 + gen * 17 + i);
        }
        return data;
    }
}
