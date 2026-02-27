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

import io.nosqlbench.vectordata.discovery.metadata.PredicateStoreBackend;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// SQLite-backed implementation of {@link PredicateStoreBackend}.
///
/// The database contains four tables:
/// - {@code predicates} — `(ordinal INTEGER PRIMARY KEY, data BLOB)`
/// - {@code result_indices} — `(ordinal INTEGER PRIMARY KEY, data BLOB)`
/// - {@code metadata_layout} — `(id INTEGER PRIMARY KEY, data BLOB)` (single row)
/// - {@code metadata_content} — `(ordinal INTEGER PRIMARY KEY, data BLOB)`
///
/// Record encoding is the same as the slabtastic backend:
/// {@link io.nosqlbench.vectordata.discovery.metadata.MetadataRecordCodec}
/// for content, PNode BBWriter for predicates.
public class SQLitePredicateBackend implements PredicateStoreBackend {

    private final Connection connection;

    /// Creates a backend from a local SQLite database file.
    ///
    /// @param dbPath the path to the `.db` or `.sqlite` file
    /// @throws SQLException if the database cannot be opened
    public SQLitePredicateBackend(Path dbPath) throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath.toAbsolutePath());
        connection.setAutoCommit(false);
        try (var stmt = connection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA synchronous=NORMAL");
        }
    }

    @Override
    public Optional<ByteBuffer> getPredicate(long ordinal) {
        return queryBlob("SELECT data FROM predicates WHERE ordinal = ?", ordinal);
    }

    @Override
    public Optional<ByteBuffer> getResultIndices(long ordinal) {
        return queryBlob("SELECT data FROM result_indices WHERE ordinal = ?", ordinal);
    }

    @Override
    public Optional<ByteBuffer> getMetadataLayout() {
        return queryBlob("SELECT data FROM metadata_layout WHERE id = ?", 0);
    }

    @Override
    public Optional<ByteBuffer> getMetadataContent(long ordinal) {
        return queryBlob("SELECT data FROM metadata_content WHERE ordinal = ?", ordinal);
    }

    @Override
    public long getPredicateCount() {
        return queryCount("predicates");
    }

    @Override
    public long getResultIndicesCount() {
        return queryCount("result_indices");
    }

    @Override
    public long getMetadataContentCount() {
        return queryCount("metadata_content");
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        // SQLite databases are local files; no prebuffering needed.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private Optional<ByteBuffer> queryBlob(String sql, long key) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, key);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    byte[] data = rs.getBytes(1);
                    if (data != null) {
                        return Optional.of(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("SQLite query failed: " + sql, e);
        }
        return Optional.empty();
    }

    private long queryCount(String table) {
        try (var stmt = connection.createStatement();
             var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            // Table may not exist
            return 0;
        }
        return 0;
    }
}
