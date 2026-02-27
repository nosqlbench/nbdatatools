package io.nosqlbench.vectordata.discovery.metadata.sqlite;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import io.nosqlbench.vectordata.discovery.metadata.MetadataLayoutImpl;
import io.nosqlbench.vectordata.discovery.metadata.MetadataRecordCodec;
import io.nosqlbench.vectordata.spec.predicates.PNode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/// Writes predicate test data to an SQLite ({@code .db}) database.
///
/// Creates four tables:
/// - {@code predicates} — `(ordinal INTEGER PRIMARY KEY, data BLOB)`
/// - {@code result_indices} — `(ordinal INTEGER PRIMARY KEY, data BLOB)`
/// - {@code metadata_layout} — `(id INTEGER PRIMARY KEY, data BLOB)`
/// - {@code metadata_content} — `(ordinal INTEGER PRIMARY KEY, data BLOB)`
///
/// Usage:
/// ```java
/// try (var writer = new SQLitePredicateWriter(path, layout)) {
///     writer.writePredicate(0, predicate);
///     writer.writeResultIndices(0, new int[]{1, 2, 3});
///     writer.writeMetadataRecord(0, Map.of("color", "red"));
/// }
/// ```
public class SQLitePredicateWriter implements AutoCloseable {

    private final Connection connection;
    private final MetadataLayoutImpl layout;
    private final PreparedStatement insertPredicate;
    private final PreparedStatement insertResultIndices;
    private final PreparedStatement insertMetadataContent;

    /// Creates a new writer targeting the given database file.
    ///
    /// The schema is created and the metadata layout is written
    /// during construction.
    ///
    /// @param dbPath the output database file
    /// @param layout the metadata layout (schema)
    /// @throws SQLException if the database cannot be opened or schema creation fails
    public SQLitePredicateWriter(Path dbPath, MetadataLayoutImpl layout) throws SQLException {
        this.layout = layout;
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath());
        connection.setAutoCommit(false);

        try (var stmt = connection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA synchronous=NORMAL");
            stmt.execute("CREATE TABLE IF NOT EXISTS predicates (ordinal INTEGER PRIMARY KEY, data BLOB)");
            stmt.execute("CREATE TABLE IF NOT EXISTS result_indices (ordinal INTEGER PRIMARY KEY, data BLOB)");
            stmt.execute("CREATE TABLE IF NOT EXISTS metadata_layout (id INTEGER PRIMARY KEY, data BLOB)");
            stmt.execute("CREATE TABLE IF NOT EXISTS metadata_content (ordinal INTEGER PRIMARY KEY, data BLOB)");
        }

        // Write layout record
        try (var ps = connection.prepareStatement(
            "INSERT OR REPLACE INTO metadata_layout (id, data) VALUES (0, ?)")) {
            ps.setBytes(1, layout.encode());
            ps.executeUpdate();
        }
        connection.commit();

        insertPredicate = connection.prepareStatement(
            "INSERT OR REPLACE INTO predicates (ordinal, data) VALUES (?, ?)");
        insertResultIndices = connection.prepareStatement(
            "INSERT OR REPLACE INTO result_indices (ordinal, data) VALUES (?, ?)");
        insertMetadataContent = connection.prepareStatement(
            "INSERT OR REPLACE INTO metadata_content (ordinal, data) VALUES (?, ?)");
    }

    /// Writes a predicate at the given ordinal.
    ///
    /// @param ordinal   the predicate ordinal
    /// @param predicate the predicate tree node
    /// @throws SQLException if writing fails
    public void writePredicate(long ordinal, PNode<?> predicate) throws SQLException {
        ByteBuffer buf = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
        predicate.encode(buf);
        buf.flip();
        byte[] data = new byte[buf.remaining()];
        buf.get(data);
        insertPredicate.setLong(1, ordinal);
        insertPredicate.setBytes(2, data);
        insertPredicate.executeUpdate();
    }

    /// Writes result indices at the given ordinal.
    ///
    /// @param ordinal the result set ordinal
    /// @param indices the matching metadata record ordinals
    /// @throws SQLException if writing fails
    public void writeResultIndices(long ordinal, int[] indices) throws SQLException {
        ByteBuffer buf = ByteBuffer.allocate(4 + indices.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(indices.length);
        for (int idx : indices) {
            buf.putInt(idx);
        }
        insertResultIndices.setLong(1, ordinal);
        insertResultIndices.setBytes(2, buf.array());
        insertResultIndices.executeUpdate();
    }

    /// Writes a metadata content record at the given ordinal.
    ///
    /// @param ordinal the record ordinal
    /// @param record  the field values keyed by name
    /// @throws SQLException if writing fails
    public void writeMetadataRecord(long ordinal, Map<String, Object> record) throws SQLException {
        byte[] encoded = MetadataRecordCodec.encode(layout, record);
        insertMetadataContent.setLong(1, ordinal);
        insertMetadataContent.setBytes(2, encoded);
        insertMetadataContent.executeUpdate();
    }

    /// Commits all pending writes.
    ///
    /// @throws SQLException if the commit fails
    public void commit() throws SQLException {
        connection.commit();
    }

    @Override
    public void close() throws SQLException {
        try {
            connection.commit();
        } finally {
            insertPredicate.close();
            insertResultIndices.close();
            insertMetadataContent.close();
            connection.close();
        }
    }
}
