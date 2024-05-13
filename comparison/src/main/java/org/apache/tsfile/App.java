package org.apache.tsfile;

import org.apache.parquet.bmtool.BMWriter;

/**
 * Hello world!
 *
 */
public class App {

    public static void main( String[] args ) throws Exception{
        System.out.println( "Hello World!" );
        org.apache.parquet.bmtool.BMWriter.main(args);
        org.apache.tsfile.bmtool.BMWriter.main(args);
    }
}
