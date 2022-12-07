package org.example.dao.dto;

import java.nio.file.Path;

public record PairedFiles(Path dataFile, Path indexesFile) {
}
