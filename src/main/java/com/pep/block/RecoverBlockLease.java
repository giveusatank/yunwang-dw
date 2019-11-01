package com.pep.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

public class RecoverBlockLease {

    public static void main(String[] args) {
        try {
            recoverlease(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void recoverlease(String path) throws IOException {
        DistributedFileSystem fs = new DistributedFileSystem();
        Configuration conf = new Configuration();
        fs.initialize(URI.create(path),conf);
        fs.recoverLease(new Path(path));
        fs.close();
    }
}
