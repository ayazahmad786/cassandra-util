package com.protectwise.cassandra.db.compaction;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;

import java.io.IOException;

/**
 * Dummy backup sink to be used for dry run
 * Created by ayaz on 13/4/17.
 */
public class DummyBackupSinkForDeletingCompaction implements IDeletedRecordsSink {
    @Override
    public void accept(OnDiskAtomIterator partition) {

    }

    @Override
    public void accept(DecoratedKey key, OnDiskAtom cell) {

    }

    @Override
    public void begin() {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void abort() {

    }
}
