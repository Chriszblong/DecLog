package com.yahoo.ycsb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import com.csvreader.CsvReader;

/**
 * Created by Andreas Bader on 16.08.15.
 */

/* Goal: Read a Huge CSV File into some sort of buffer
*  One Thread for reading CSV File in background
*  Idea: Always have a buffer filled with actual lines from csv file
*  but instead of reading data chunks-wise from file
*  read it on-the-fly in a backgroudn thread
*  and keep the buffer filled
*/

public class ThreadedCSVReader extends Thread implements Iterator<String[]> {

    private int bufferSize = 1000;
    private String[][] al;
    private CsvReader csvr;
    private String[] headers;
    private boolean csvend = false;
    private int readPointer = -1;
    private int writePointer = 0;

    public ThreadedCSVReader(int bufferSize, String csvFile, Boolean start) {
        this.bufferSize = bufferSize;
        if (this.bufferSize <= 2) {
            System.err.println("WARNING: bufferSize should be at least 3. Defaulting to 3.");
            this.bufferSize=3;
        }
        try {
            this.csvr = new CsvReader(csvFile);
            csvr.readHeaders();
            this.headers = csvr.getHeaders();
        }
        catch (FileNotFoundException e) {
            System.err.println("ERROR: CSV file " + csvFile + " not found.");
            e.printStackTrace();
            System.exit(-1);
        }
        catch (IOException e) {
            System.err.println("ERROR: Can't read headers of CSV file" + csvFile + ".");
            e.printStackTrace();
            System.exit(-1);
        }

        this.al = new String[this.bufferSize][this.headers.length];
        if (start) {
            this.start();
        }
    }

    public String[] getHeaders() {
        return this.headers;
    }

    public void run() {
        try {
            while (this.csvr.readRecord()){
                while ((this.writePointer == this.bufferSize && (this.readPointer == 0 || this.readPointer == -1)) || (this.writePointer == this.readPointer)) {
                    Thread.sleep(1);
                }
                if (this.writePointer == this.bufferSize) {
                    this.writePointer = 0;
                }
                al[this.writePointer] = csvr.getValues();
                while (this.writePointer+1 == this.readPointer) {
                    Thread.sleep(1);
                }
                this.writePointer++;
            }
            this.csvend = true;
            csvr.close();
        }
        catch (IOException e) {
            System.err.println("ERROR: while reading CSV file.");
            e.printStackTrace();
            System.exit(-1);
        }
        catch (InterruptedException e) {
            System.err.println("ERROR: while reading CSV file #2.");
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        if (this.csvend && this.readPointer == this.writePointer-1) {
            return false;
        }
        return true;
    }

    @Override
    public String[] next() {
        if (! this.csvend) {
            while (this.readPointer+1 == this.writePointer) {
                try {
                    Thread.sleep(1);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (this.readPointer == this.bufferSize-1) {
                if (this.writePointer > 0) {
                    this.readPointer = -1;
                }
                else {
                    while (this.writePointer <= 0) {
                        try {
                            Thread.sleep(1);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    this.readPointer = -1;
                }
            }
        }
        if (this.readPointer < this.bufferSize-1) {
            readPointer++;
        }
        return al[this.readPointer];
    }

    @Override
    public void remove() {

    }
}
