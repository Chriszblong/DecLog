/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 *
 * @author cooperb
 *
 */
public class OneMeasurementHistogram extends OneMeasurement
{
    public static final String BUCKETS="histogram.buckets";
    public static final String BUCKETS_DEFAULT="1000";
    public static final String DEFINITION="histogram.definition";
    public static final String DEFINITION_DEFAULT="100";


    int _definition;
    int _buckets;
    int[] histogram;
    int histogramoverflow;
    int operations;
    long totallatency;

    //keep a windowed version of these stats for printing status
    int windowoperations;
    long windowtotallatency;

    long min;
    long max;

    public OneMeasurementHistogram(String name, Properties props)
    {
        super(name);
        _buckets=Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
        _definition=Integer.parseInt(props.getProperty(DEFINITION, DEFINITION_DEFAULT));
        histogram=new int[_buckets];
        histogramoverflow=0;
        operations=0;
        totallatency=0;
        windowoperations=0;
        windowtotallatency=0;
        min=-1;
        max=-1;
    }

    /* (non-Javadoc)
     * @see com.yahoo.ycsb.OneMeasurement#measure(long)
     */
    public synchronized void measure(long latency)
    {
        if (latency/_definition>=_buckets)
        {
            histogramoverflow++;
        }
        else
        {
            histogram[(int)(latency/_definition)]++;
        }
        operations++;
        totallatency+=latency;
        windowoperations++;
        windowtotallatency+=latency;

        if ( (min<0) || (latency<min) )
        {
            min=latency;
        }

        if ( (max<0) || (latency>max) )
        {
            max=latency;
        }
    }

    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException
    {
        exporter.write(getName(), "Operations", operations);
        exporter.write(getName(), "AverageLatency(us)", (((double)totallatency)/((double)operations)));
        exporter.write(getName(), "MinLatency(us)", min);
        exporter.write(getName(), "MaxLatency(us)", max);

        int opcounter=0;
        boolean done95th=false;
        for (int i=0; i<_buckets; i++)
        {
            opcounter+=histogram[i];
            if ( (!done95th) && (((double)opcounter)/((double)operations)>=0.95) )
            {
                exporter.write(getName(), "95thPercentileLatency(us)", i*_definition);
                done95th=true;
            }
            if (((double)opcounter)/((double)operations)>=0.99)
            {
                exporter.write(getName(), "99thPercentileLatency(us)", i*_definition);
                break;
            }
        }

        exportReturnCodes(exporter);

        for (int i=0; i<_buckets; i++)
        {
            exporter.write(getName(), Integer.toString(i), histogram[i]);
        }
        exporter.write(getName(), ">"+_buckets, histogramoverflow);
    }

    @Override
    public String getSummary() {
        if (windowoperations==0)
        {
            return "";
        }
        DecimalFormat d = new DecimalFormat("#.##");
        double report=((double)windowtotallatency)/((double)windowoperations);
        windowtotallatency=0;
        windowoperations=0;
        return "["+getName()+" AverageLatency(us)="+d.format(report)+"]";
    }
}