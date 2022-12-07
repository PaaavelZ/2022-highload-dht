package org.example.dao.aliases;

import org.example.dao.dto.PairedFiles;

public class FileDiskSSTable
        extends DiskSSTable<PairedFiles> {

    public FileDiskSSTable(int key, PairedFiles value) {
        super(key, value);
    }
}
