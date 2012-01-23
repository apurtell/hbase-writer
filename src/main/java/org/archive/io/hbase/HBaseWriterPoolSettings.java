package org.archive.io.hbase;

import java.io.File;
import java.util.List;

import org.archive.io.WriterPoolSettings;

public class HBaseWriterPoolSettings implements WriterPoolSettings {

  @Override
  public long getMaxFileSizeBytes() {
    return 20 * 1024 * 1024;
  }

  @Override
  public String getPrefix() {
    return null;
  }

  @Override
  public String getTemplate() {
    return null;
  }

  @Override
  public List<File> calcOutputDirs() {
    return null;
  }

  @Override
  public boolean getCompress() {
    return false;
  }

  @Override
  public List<String> getMetadata() {
    return null;
  }

  @Override
  public boolean getFrequentFlushes() {
    return true;
  }

  @Override
  public int getWriteBufferSize() {
    return 32 * 1024;
  }
}
