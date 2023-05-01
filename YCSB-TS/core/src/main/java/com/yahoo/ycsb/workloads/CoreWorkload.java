package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

/**
 * The core benchmark scenario for Timeseries Databases. Represents a set of clients doing simple CRUD operations. The relative
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * Consider one record in a Timeseries Database as following ([] is optional):
 * <metric> <timestamp> <value> [<tag1>=<tagvalue1> <tag2>=<tagvalue2> ... <tagN>=<tagvalueN>]
 * tagnames will always be TAG1,TAG2,... ,TAGN
 * tagvalus will be random strings
 * value will be a random double
 * metric is defined as 'usermetric'
 * timestamp is automatically generated out of long numbers (which represent milliseconds since 1970)
 * <p/>
 * Properties to control the client:
 * <UL>
 * <LI><b>metric</b>: name of the metric to use (default: usermetric)
 * <LI><b>tagcount</b>: the number of tags per record (default: 3)
 * <LI><b>tagvaluelengthdistribution</b>: what distribution should be used to set tag length? Options are "uniform", "zipfian" (favoring short records), "constant", and "histogram".
 * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the fieldlength property.  If "histogram", then the
 * histogram will be read from the filename specified in the "fieldlengthhistogram" property. (default: constant)
 * <LI><b>tagvaluelength</b>: the size of each tag value (default: 10)
 * <LI><b>tagprefix</b>: the prefix of each tag (only use A-Z, a-z) (default: TAG)
 * <LI><b>randomtagamount</b>: if false, then there will be always all (tagcount) tags used for every record, if true, then there will be randomly choose between 0 and tagcount (default: false)
 * <LI><b>predefinedtagvalues</b>: if true then there are predefined (randomly generated) values for every tag for every record. Instead of generating a random value everytime there will be a randomly chosen value out of the predefined list for this tag. (Consider that if you set this to false, than read/sum/avg/scan/count will produce strange results, as you then only have 1 record per tag and ycsb-ts will do read/sum/scan/count only based on timespan which does not reflect the performance of a TSDB in a good way.) (default: true)
 * <LI><b>predefinedtagvalueamount</b>: how many values should be predefined (randomly generated) for every tag? (Don't set this to high, otherwise you get in the range of 1 Tag per Record, see comment on 'predefinedtagvalues' parameter) (default: 100)
 * <LI><b>predefinedtagstoragefile</b>: Between RUN and LOAD the tags/tagvalues will be temporarly stored in this file (default: predefinedtags.txt)
 * <LI><b>predefinedtagstoreused</b>: Only stores actually used combos, this means that there can't be any misses at all while RUN Phase (default: true)
 * <LI><b>valuemax</b>: which should be the highest random value possible? (default: 10000)
 * <LI><b>valuemin</b>: which should be the lowest random value possible?  (default: 0)
 * <LI><b>taglengthhistogram</b>: filename containing the tag length histogram (only used if tagvaluelengthdistribution is "histogram"). (default: hist.txt)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 1)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>avgproportion</b>: what proportion of operations should be avg (default: 0)
 * <LI><b>countproportion</b>: what proportion of operations should be count (default: 0)
 * <LI><b>sumproportion</b>: what proportion of operations should be sum (default: 0)
 * <LI><b>timevalue</b>: which timevalue to use for avg/count/sum, zero means return one value over the whole timespan used and only use one "bucket" (default: 0)
 * <LI><b>timeunit</b>: which timeunit to use for avg/count/sum (possible: ms, s, m, h, d) (default: ms)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform (default: uniform)
 * <LI><b>maxscanlength</b>: On a single scan/avg/count/sum, the maximum period (in milliseconds) to access (default: 1000)
 * <LI><b>minscanlength</b>: On a single scan/avg/count/sum, the maximum period (in milliseconds) to access (default: 500)
 * <LI><b>scanlengthdistribution</b>: for scan/avg/count/sum, what distribution should be used to choose the amount of time to scan/avg/count/sum, for each scan/avg/count/sum, between 1 and maxscanlength (default: uniform)
 * <LI><b>maxrecordsperts</b>: should there be more than one value per timestamp possible. Has no effect if randomamountperts=tsrandom. (e.g two sensors at the same time with different tags?) (default: 10)
 * <LI><b>randomamountperts</b>: the amount of values per timestamp -> tsrandom, random or constant? random: 1 to maxrecordsperts records for every timestamp, constant: always maxrecordsperts records per timestamp, tsrandom: randomly choose timestamp and add record (random recordamount between 0 to recordcount per timestamp) (default: constant)
 * <LI><b>randomfloorfilling</b>: when set to true and using randomamountperts = tsrandom, every timestamp will at least have 1 record after Load Phase, benchmarking will be wrong and recordcount must be greater than (insertstart-insertend) (first record per ts will not be generated randomly) (default: false)
 * <LI><b>querymaxtagvalues</b>: for read/scan/avg/count/sum: how many tagvalues should be given for each tag to look for? (it will be randomly chosed how many tags are used and for each tag randomly between 1 and querymaxtagvalues how many tagvalues)(default: 10)
 * <LI><b>notagsforread</b>: don't use tags for read, as it is quite hopeless (searching for one timestamp and matching the right tags -> not really possible)(default: true)
 * <LI><b>insertend</b>: The end of the insertion time space (exclusive) (default=1439242005000)
 * <LI><b>insertstart</b>: The start of the insertion time space (inclusive) (default=1439241005000)
 * <LI><b>readfromcsv</b>: Read from values from csv file (default=false)
 * <LI><b>csvfile</b>: Path to csv file (default=values.csv)
 * <LI><b>csvbuffersize</b>: Buffer size for csv reader (default=100000)
 * <LI><b>description</b>: Description for the workload (default=Workloaddescription)
 * <LI><b>timeresolution</b>: Size of one timestep in milliseconds (1 = 1 ms, 1000 = 1 s,..) (default=1000)
 * </ul>
 */
public class CoreWorkload extends Workload {

    /**
     * The name of the database metric to run queries against.
     * Needs also to be set in DruidClient.java
     */
    public static final String METRICNAME_PROPERTY = "metric";

    /**
     * The default name of the database metric to run queries against.
     * Needs also to be set in DruidClient.java and H5ServClient.java
     */
    public static final String METRICNAME_PROPERTY_DEFAULT = "usermetric";
    /**
     * The name of the property for the max number of fields in a record.
     * Needs also to be set in DruidClient.java and H5ServClient.java
     */
    public static final String TAG_COUNT_PROPERTY = "tagcount";
    /**
     * Max number of fields in a record.
     * Needs also to be set in DruidClient.java and H5ServClient.java
     */
    public static final String TAG_COUNT_PROPERTY_DEFAULT = "3";
    /**
     * The name of the property for the tag length distribution. Options are "uniform", "zipfian" (favoring short records), "constant", and "histogram".
     * <p/>
     * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the fieldlength property.  If "histogram", then the
     * histogram will be read from the filename specified in the "fieldlengthhistogram" property.
     */
    public static final String TAG_VALUE_LENGTH_DISTRIBUTION_PROPERTY = "tagvaluelengthdistribution";
    /**
     * The default field length distribution.
     */
    public static final String TAG_VALUE_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";
    /**
     * The name of the property for the length of a field in bytes.
     * Needs also to be set in H5ServClient.java
     */
    public static final String TAG_VALUE_LENGTH_PROPERTY = "tagvaluelength";
    /**
     * The default maximum length of a field in bytes.
     * Needs also to be set in H5ServClient.java
     */
    public static final String TAG_VALUE_LENGTH_PROPERTY_DEFAULT = "10";
    /**
     * The name of the property for the tag prefix
     * Needs also to be set in DruidClient.java and H5ServClient.java
     */
    public static final String TAG_PREFIX_PROPERTY = "tagprefix";
    /**
     * The default tag prefix
     * Needs also to be set in DruidClient.java and H5ServClient.java
     */
    public static final String TAG_PREFIX_PROPERTY_DEFAULT = "TAG";
    /**
     * The name of the property for deciding whether to use always all 10 tags or vary between 1 and TAG_COUNT_PROPERTY
     */
    public static final String RANDOM_TAG_AMOUNT_PROPERTY = "randomtagamount";
    /**
     * The name of the property for deciding whether to use always all 10 tags or vary between 1 and TAG_COUNT_PROPERTY
     */
    public static final String RANDOM_TAG_AMOUNT_PROPERTY_DEFAULT = "false";
    /**
     * The name of the property for deciding whether to use a tag randomly out of a predefined tag space or to randomly generate a new tag every time
     */
    public static final String PREDEFINED_TAG_VALUES_PROPERTY = "predefinedtagvalues";
    /**
     * Default value of the property for deciding whether to use a tag randomly out of a predefined tag space or to randomly generate a new tag every time
     */
    public static final String PREDEFINED_TAG_VALUES_PROPERTY_DEFAULT = "true";
    /**
     * The name of the property for the amount of predefined tags
     */
    public static final String PREDEFINED_TAG_VALUE_AMOUNT_PROPERTY = "predefinedtagvalueamount";
    /**
     * The default amount of predefined tags
     */
    public static final String PREDEFINED_TAG_VALUE_AMOUNT_PROPERTY_DEFAULT = "100";
    /**
     * The name of the file were predefined tags/tagvalues  between runs are stored
     */
    public static final String PREDEFINED_TAG_STORAGE_FILE_PROPERTY = "predefinedtagstoragefile";
    /**
     * The default name of the file were predefined tags/tagvalues  between runs are stored
     */
    public static final String PREDEFINED_TAG_STORAGE_FILE_PROPERTY_DEFAULT = "predefinedtags.txt";
    /**
     * The name of the property for deciding whether to only store actually used tagvalue-combos (no misses while RUN Phase)
     */
    public static final String PREDEFINED_TAG_STORE_USED_PROPERTY = "predefinedtagstoreused";
    /**
     * Default value of the property for deciding whether to only store actually used tagvalue-combos (no misses while RUN Phase)
     */
    public static final String PREDEFINED_TAG_STORE_USED_PROPERTY_DEFAULT = "true";
    /**
     * The name of the property for the maximum value of a time series value
     */
    public static final String VALUE_MAX_PROPERTY = "valuemax";
    /**
     * The default maximum length of a maximum value of a time series value
     */
    public static final String VALUE_MAX_PROPERTY_DEFAULT = "10000";
    /**
     * The name of the property for the minimum value of a time series value
     */
    public static final String VALUE_MIN_PROPERTY = "valuemin";
    /**
     * The default maximum length of a minimum value of a time series value
     */
    public static final String VALUE_MIN_PROPERTY_DEFAULT = "0";
    /**
     * The name of a property that specifies the filename containing the tag length histogram (only used if fieldlengthdistribution is "histogram").
     */
    public static final String TAG_LENGTH_HISTOGRAM_FILE_PROPERTY = "taglengthhistogram";
    /**
     * The default filename containing a field length histogram.
     */
    public static final String TAG_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";
    /**
     * The name of the property for the proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";
    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "1.0";
    /**
     * The name of the property for the proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";
    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";
    /**
     * The name of the property for the proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";
    /**
     * The default proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";
    /**
     * The name of the property for the proportion of transactions that are avgs.
     */
    public static final String AVG_PROPORTION_PROPERTY = "avgproportion";
    /**
     * The default proportion of transactions that are avgs.
     */
    public static final String AVG_PROPORTION_PROPERTY_DEFAULT = "0.0";
    /**
     * The name of the property for the proportion of transactions that are sums.
     */
    public static final String SUM_PROPORTION_PROPERTY = "sumproportion";
    /**
     * The default proportion of transactions that are sums.
     */
    public static final String SUM_PROPORTION_PROPERTY_DEFAULT = "0.0";
    /**
     * The name of the property for the proportion of transactions that are counts.
     */
    public static final String COUNT_PROPORTION_PROPERTY = "countproportion";
    /**
     * The default proportion of transactions that are counts.
     */
    public static final String COUNT_PROPORTION_PROPERTY_DEFAULT = "0.0";
    /**
     * The name of the property for the proportion of transactions that are scans.
     */
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";
    /**
     * The default proportion of transactions that are scans.
     */
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.0";
    /**
     * The name of the property for the the distribution of requests across the keyspace. Options are "uniform" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
    /**
     * The default distribution of requests across the keyspace
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";
    /**
     * The name of the property for the max scan length (number of milliseconds)
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
    /**
     * The name of the property for the min scan length (number of milliseconds)
     */
    public static final String MIN_SCAN_LENGTH_PROPERTY = "minscanlength";

    /**
     * The name of the property for deciding how many records per timestamp are allowed
     */
    public static final String MAX_RECORD_PER_TS_PROPERTY = "maxrecordsperts";
    /**
     *  The default value of the property for deciding how many records per timestamp are allowed
     */
    public static final String MAX_RECORD_PER_TS_PROPERTY_DEFAULT = "10";

    /**
     * The name of the property for deciding how many tagvalues per tag for scan/avg/count/sum/read
     */
    public static final String QUERY_MAX_TAG_PROPERTY = "querymaxtagvalues";
    /**
     *  The default value for deciding how many tagvalues per tag for scan/avg/count/sum/read
     */
    public static final String QUERY_MAX_TAG_PROPERTY_DEFAULT = "10";
    /**
     * The name of the property for deciding if read should use tags
     */
    public static final String NO_TAGS_FOR_READ_PROPERTY = "notagsforread";
    /**
     *  The default value for deciding if read should use tags
     */
    public static final String NO_TAGS_FOR_READ_PROPERTY_DEFAULT = "true";

    /**
     * The method with which the amount of records per timestamp will be generated
     */
    public static final String RANDOM_AMOUNT_PER_TS_AMOUNT_PROPERTY = "randomamountperts";
    /**
     * The default method  with which the amount of records per timestamp will be generated
     */
    public static final String RANDOM_AMOUNT_PER_TS_AMOUNT_PROPERTY_DEFAULT = "constant";
    /**
     * The name of the property for deciding if using a minimum amount of records per timestamp when using randomamountperts=tsrandom
     */
    public static final String RANDOM_FLOORFILLING_AMOUNT_PROPERTY = "randomfloorfilling";
    /**
     * The default value for the property for deciding if using a minimum amount of records per timestamp when using randomamountperts=tsrandom
     */
    public static final String RANDOM_FLOORFILLING_AMOUNT_PROPERTY_DEFAULT = "false";

    /**
     * The default max scan length.
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";
    /**
     * The default min scan length.
     */
    public static final String MIN_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";
    /**
     * The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
    /**
     * The default max scan length.
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the time value for avg/count/sum function
     */
    public static final String TIME_VALUE_PROPERTY = "timevalue";
    /**
     * The default value for the time value for avg/count/sum function property
     */
    public static final String TIME_VALUE_PROPERTY_DEFAULT = "0";

    /**
     * The name of the property for the time unit for avg/count/sum function
     */
    public static final String TIME_UNIT_PROPERTY = "timeunit";
    /**
     * The default value for the time unit for avg/count/sum function property
     */
    public static final String TIME_UNIT_PROPERTY_DEFAULT = "ms";

    /**
     * The name of the property for deciding if values are red from a csv file
     */
    public static final String READ_FROM_CSV_FILE_PROPERTY = "readfromcsv";
    /**
     * The default value for the property for deciding if values are red from a csv file
     */
    public static final String READ_FROM_CSV_FILE_PROPERTY_DEFAULT = "false";

    /**
     * The name of the property for csv file path
     */
    public static final String CSV_FILE_PROPERTY = "csvfile";
    /**
     * The default csv file path
     */
    public static final String CSV_FILE_PROPERTY_DEFAULT = "values.csv";

    /**
     * The name of the property for sv buffer size
     */
    public static final String CSV_BUFFER_SIZE_PROPERTY = "csvbuffersize";
    /**
     * The default csv buffer size
     */
    public static final String CSV_BUFFER_SIZE_PROPERTY_DEFAULT = "10000";

    /**
     * The name of the property for workload description
     */
    public static final String DESCRIPTION_PROPERTY = "description";
    /**
     * The default workload description
     */
    public static final String DESCRIPTION_PROPERTY_DEFAULT = "Workloaddescription";

    /**
     * The name of the property for time resolution
     */
    public static final String TIME_RESOLUTION_PROPERTY = "timeresolution";
    /**
     * The default time resolution
     */
    public static final String TIME_RESOLUTION_PROPERTY_DEFAULT = "1000";

    public static String metric;
    int tagcount;
    /**
     * Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
     */
    LongGenerator taglengthgenerator;
    LongGenerator keysequence;
    DiscreteGenerator operationchooser;
    LongGenerator keychooser;
    Generator fieldchooser;
    LongGenerator scanlength;
    int recordcount;
    private List<String> tagnames;
    private HashMap<String, ArrayList<String>> tagvalues;
    private String tagPrefix;
    private boolean randomtagamount;
    private boolean predefinedtagvalues;
    private int predefinedtagvaluesamount;
    private int valuemax;
    private int valuemin;
    private Measurements _measurements = Measurements.getMeasurements();
    private long insertstart;
    private long insertend;
    private Random rand = null;
    private int maxrecordsperts;
    private int maxrecordspertcounter;
    private String randomamountperts;
    private int maxrecordcounter = 0;
    private long keynum = 0;
    private long keynum2 = 0;
    private boolean randomfloorfilling = false;
    private int floorcounter = 0;
    private int querymaxtagvalues = 0;
    private boolean notagsforread = false;
    private File predefinedtagstoragefile;
    private boolean predefinedtagstoreused = false;
    private ArrayList<String[]> usedTags;
    private Boolean tagvaluesLoaded = false;
    private int timevalue;
    private TimeUnit timeunit;
    private boolean readfromcsv;
    private String csvfile;
    private int csvbuffersize;
    private ThreadedCSVReader tscr;
    private String description;
    private int timeResolution;
    private AcknowledgedCounterGenerator transactioninsertkeysequence;

    protected static LongGenerator getTagLengthGenerator(Properties p) throws WorkloadException {
        LongGenerator taglengthgenerator;
        String taglengthdistribution = p.getProperty(TAG_VALUE_LENGTH_DISTRIBUTION_PROPERTY, TAG_VALUE_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        long taglength = Integer.parseInt(p.getProperty(TAG_VALUE_LENGTH_PROPERTY, TAG_VALUE_LENGTH_PROPERTY_DEFAULT));
        String taglengthhistogram = p.getProperty(TAG_LENGTH_HISTOGRAM_FILE_PROPERTY, TAG_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (taglengthdistribution.compareTo("constant") == 0) {
            taglengthgenerator = new ConstantLongGenerator(taglength);
        }
        else if (taglengthdistribution.compareTo("uniform") == 0) {
            taglengthgenerator = new UniformLongGenerator(1, taglength);
        }
        else if (taglengthdistribution.compareTo("zipfian") == 0) {
            taglengthgenerator = new ZipfianGenerator(1, taglength);
        }
        else if (taglengthdistribution.compareTo("histogram") == 0) {
            try {
                taglengthgenerator = new HistogramGenerator(taglengthhistogram);
            }
            catch (IOException e) {
                throw new WorkloadException("Couldn't read field length histogram file: " + taglengthhistogram, e);
            }
        }
        else {
            throw new WorkloadException("Unknown field length distribution \"" + taglengthdistribution + "\"");
        }
        return taglengthgenerator;
    }


    /** Writing usedTags
     * if predefinedtagstoreused is set
     */
    public void cleanup() {
        if (predefinedtagstoreused && ! this.tagvaluesLoaded) {
            if (!predefinedtagstoragefile.exists() && !predefinedtagstoragefile.isDirectory() && !predefinedtagstoragefile.isFile()) {
                try {
                    FileOutputStream fileOut = new FileOutputStream(predefinedtagstoragefile);
                    ObjectOutputStream oos = new ObjectOutputStream(fileOut);
                    oos.writeObject(usedTags);
                }
                catch (Exception e) {
                    System.err.println("ERROR: Can't write tagvalues to predefinedtagsotragefile " + predefinedtagstoragefile.getAbsolutePath() + ".");
                    e.printStackTrace();
                }
            }
            else {
                System.err.println("ERROR: Can't write tagvalues to predefinedtagsotragefile " + predefinedtagstoragefile.getAbsolutePath() + ".");
            }
        }
    }

    /**
     * Initialize the scenario.
     * Called once, in the main client thread, before any operations are started.
     */
    public void init(Properties p) throws WorkloadException {
        metric = p.getProperty(METRICNAME_PROPERTY, METRICNAME_PROPERTY_DEFAULT);
        tagcount = Integer.parseInt(p.getProperty(TAG_COUNT_PROPERTY, TAG_COUNT_PROPERTY_DEFAULT));
        tagPrefix = p.getProperty(TAG_PREFIX_PROPERTY, TAG_PREFIX_PROPERTY_DEFAULT);
        tagnames = new ArrayList<String>();
        for (int i = 0; i < tagcount; i++) {
            tagnames.add(tagPrefix + i);
        }
        taglengthgenerator = CoreWorkload.getTagLengthGenerator(p);
        description = p.getProperty(DESCRIPTION_PROPERTY, DESCRIPTION_PROPERTY_DEFAULT);
        if (description != "") {
            System.err.println("[DESCRIPTION], " + description);
        }
        else {
            System.err.println("[DESCRIPTION], Test");
        }
        double readproportion = Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        double scanproportion = Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
        double avgproportion = Double.parseDouble(p.getProperty(AVG_PROPORTION_PROPERTY, AVG_PROPORTION_PROPERTY_DEFAULT));
        double sumproportion = Double.parseDouble(p.getProperty(SUM_PROPORTION_PROPERTY, SUM_PROPORTION_PROPERTY_DEFAULT));
        double countproportion = Double.parseDouble(p.getProperty(COUNT_PROPORTION_PROPERTY, COUNT_PROPORTION_PROPERTY_DEFAULT));
        double updateproportion = Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        readfromcsv = Boolean.parseBoolean(p.getProperty(READ_FROM_CSV_FILE_PROPERTY, READ_FROM_CSV_FILE_PROPERTY_DEFAULT));
        csvfile = p.getProperty(CSV_FILE_PROPERTY, CSV_FILE_PROPERTY_DEFAULT);
        csvbuffersize = Integer.parseInt(p.getProperty(CSV_BUFFER_SIZE_PROPERTY, CSV_BUFFER_SIZE_PROPERTY_DEFAULT));
        if (csvbuffersize <= 2) {
            System.err.println("WARNING: csvbuffersize should be at least 3. Defaulting to 3.");
            csvbuffersize=3;
        }
        if (readfromcsv) {
            try {
                File f = new File(csvfile);
                if (!f.isFile() || !f.canRead()) {
                    System.out.println("ERROR: Can't read " + csvfile + ". Defaulting to generated values.");
                    readfromcsv = false;
                }
            }
            catch (Exception e) {
                System.out.println("ERROR: Can't read " + csvfile + ". Defaulting to generated values.");
                readfromcsv = false;
            }
        }
        if (readfromcsv) {
            tscr = new ThreadedCSVReader(csvbuffersize,csvfile,false);
            String[] tmpArr = tscr.getHeaders();
            for (int i = 0; i < tmpArr.length; i++){
                if (! tagnames.contains(tmpArr[i])) {
                    System.err.println("ERROR: CSV file does contain wrong tag " + tmpArr[i] + ". Defaulting to generated tags/tagvalues.");
                    readfromcsv = false;
                }
            }
            int counter = 0;
            for (String tag : tagnames) {
                try {
                    if (!tmpArr[counter].equals(tag)) {
                        System.err.println("ERROR: CSV file does not contain tag " + tag + ". Defaulting to generated tags/tagvalues.");
                        readfromcsv = false;
                    }
                }
                catch (Exception e) {
                    System.err.println("ERROR: CSV file has not enoug tags." + ". Defaulting to generated tags/tagvalues.");
                    readfromcsv = false;
                }
                counter++;
            }
        }
        timevalue = Integer.parseInt(p.getProperty(TIME_VALUE_PROPERTY, TIME_VALUE_PROPERTY_DEFAULT));

        String tmpTimeUnit = p.getProperty(TIME_UNIT_PROPERTY, TIME_UNIT_PROPERTY_DEFAULT);
        if (tmpTimeUnit.toLowerCase().equals("ms")) {
            timeunit = TimeUnit.MILLISECONDS;
        }
        else if (tmpTimeUnit.toLowerCase().equals("s")) {
            timeunit = TimeUnit.SECONDS;
        }
        else if (tmpTimeUnit.toLowerCase().equals("m")) {
            timeunit = TimeUnit.MINUTES;
        }
        else if (tmpTimeUnit.toLowerCase().equals("d")) {
            timeunit = TimeUnit.DAYS;
        }
        else if (tmpTimeUnit.toLowerCase().equals("h")) {
            timeunit = TimeUnit.HOURS;
        }
        else {
            System.err.println("ERROR: Unknown timeUnit " + tmpTimeUnit + ". Defaulting to s.");
            timeunit = TimeUnit.SECONDS;
        }
        recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
        if (recordcount == 0)
            recordcount = Integer.MAX_VALUE;
        String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int maxscanlength = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
        int minscanlength = Integer.parseInt(p.getProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_PROPERTY_DEFAULT));
        if (minscanlength > maxscanlength) {
            System.err.println("WARNING: minscanlength > maxscanlength. Setting minscanlength = maxscanlangth (full timespan).");
            minscanlength = maxscanlength;
        }
        String scanlengthdistrib = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        maxrecordsperts = Integer.parseInt(p.getProperty(MAX_RECORD_PER_TS_PROPERTY, MAX_RECORD_PER_TS_PROPERTY_DEFAULT));
        querymaxtagvalues = Integer.parseInt(p.getProperty(QUERY_MAX_TAG_PROPERTY, QUERY_MAX_TAG_PROPERTY_DEFAULT));
        if (querymaxtagvalues == 0){
            querymaxtagvalues = 1;
            System.err.println("WARNING: Querymaxtagvalues should be at least one. Setting it to one.");
        }
        randomfloorfilling = Boolean.parseBoolean(p.getProperty(RANDOM_FLOORFILLING_AMOUNT_PROPERTY, RANDOM_FLOORFILLING_AMOUNT_PROPERTY_DEFAULT));
        notagsforread = Boolean.parseBoolean(p.getProperty(NO_TAGS_FOR_READ_PROPERTY, NO_TAGS_FOR_READ_PROPERTY_DEFAULT));
        randomamountperts = p.getProperty(RANDOM_AMOUNT_PER_TS_AMOUNT_PROPERTY, RANDOM_AMOUNT_PER_TS_AMOUNT_PROPERTY_DEFAULT);
        maxrecordcounter = maxrecordsperts+1;
        if ( this.randomamountperts.compareTo("constant") == 0 ) {
            this.maxrecordspertcounter = this.maxrecordsperts;
        }

        insertstart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
        insertend  = Long.parseLong(p.getProperty(INSERT_END_PROPERTY, INSERT_END_PROPERTY_DEFAULT));
        timeResolution  = Integer.parseInt(p.getProperty(TIME_RESOLUTION_PROPERTY, TIME_RESOLUTION_PROPERTY_DEFAULT));
        if (timeResolution >= Math.abs(insertend-insertstart)) {
            System.err.println("ERROR: timeresolution (" + timeResolution + ") is bigger than insertend-insertstart (" + Math.abs(insertend-insertstart) + "), can't do one time step.");
            System.exit(-1);
        }
        insertend=insertend/timeResolution;
        insertstart=insertstart/timeResolution;
        if (recordcount < Math.abs(insertend-insertstart) &&  this.randomamountperts.compareTo("tsrandom") != 0) {
            System.err.println("WARNING: recordcount is smaller than (insertend-insertstart) and randomamountperts is not set to 'tsrandom' .. fixing insertend to (insertstart+recordcount)");
            insertend = insertstart+recordcount;
        }
        if (insertstart >= insertend) {
            System.err.println("WARNING: insertstart is greater or equal than insertend .. fixing insertend to (insertstart+recordcount).");
            insertend = insertstart+recordcount;
        }
        valuemax = Integer.parseInt(p.getProperty(VALUE_MAX_PROPERTY, VALUE_MAX_PROPERTY_DEFAULT));
        valuemin = Integer.parseInt(p.getProperty(VALUE_MIN_PROPERTY, VALUE_MIN_PROPERTY_DEFAULT));
        randomtagamount = Boolean.parseBoolean(p.getProperty(RANDOM_TAG_AMOUNT_PROPERTY, RANDOM_TAG_AMOUNT_PROPERTY_DEFAULT));
        rand = new Random();
        predefinedtagstoreused = Boolean.parseBoolean(p.getProperty(PREDEFINED_TAG_STORE_USED_PROPERTY, PREDEFINED_TAG_STORE_USED_PROPERTY_DEFAULT));
        predefinedtagvalues = Boolean.parseBoolean(p.getProperty(PREDEFINED_TAG_VALUES_PROPERTY, PREDEFINED_TAG_VALUES_PROPERTY_DEFAULT));
        predefinedtagvaluesamount = Integer.parseInt(p.getProperty(PREDEFINED_TAG_VALUE_AMOUNT_PROPERTY, PREDEFINED_TAG_VALUE_AMOUNT_PROPERTY_DEFAULT));
        if (predefinedtagvaluesamount == 0)
            throw new WorkloadException("Can't work with 0 predefined tags.");
        if (predefinedtagvalues && predefinedtagvaluesamount < querymaxtagvalues) {
            predefinedtagvaluesamount = querymaxtagvalues;
            System.err.println("WARNING: Predefinedtagvaluesamount should be at least as high as querymaxtagvalues. Setting it to querymaxtagvalues.");
        }
        tagvaluesLoaded = false;
        try {
            predefinedtagstoragefile = new File(p.getProperty(PREDEFINED_TAG_STORAGE_FILE_PROPERTY, PREDEFINED_TAG_STORAGE_FILE_PROPERTY_DEFAULT));
            if (predefinedtagstoragefile.exists() && !predefinedtagstoragefile.isDirectory() && predefinedtagstoragefile.isFile() && predefinedtagstoragefile.canRead()) {
                FileInputStream fileIn = new FileInputStream(predefinedtagstoragefile);
                ObjectInputStream ois = new ObjectInputStream(fileIn);
                if (! predefinedtagstoreused) {
                    tagvalues = (HashMap<String, ArrayList<String>>) ois.readObject();
                    tagvaluesLoaded = true;
                    predefinedtagstoragefile.delete();
                    for (String tag : tagnames) {
                        if (! tagvalues.containsKey(tag)) {
                            System.err.println("WARNING: " + predefinedtagstoragefile.getAbsolutePath() + " does not match tagnames. "+ tag + " is missing. Using random Tagvalues.");
                            tagvaluesLoaded = false;
                        }
                    }
                }
                else {

                    usedTags = (ArrayList<String[]>) ois.readObject();
                    tagvaluesLoaded = true;
                    predefinedtagstoragefile.delete();
                }
                ois.close();
                fileIn.close();
            }
            else {
                tagvaluesLoaded = false;
            }
        }
        catch (Exception e) {
            System.err.println("ERROR: Can't load predefinedtagsotragefile " + predefinedtagstoragefile.getAbsolutePath() + ". Using random tagvalues!");
            e.printStackTrace();
            tagvaluesLoaded = false;
        }
        if (readfromcsv && ! tagvaluesLoaded) {
            tscr.start();
        }
        if (predefinedtagvalues && ( !tagvaluesLoaded || predefinedtagstoreused) && ! readfromcsv) {
            tagvalues = new HashMap<String, ArrayList<String>>();
            for (String tag : tagnames) {
                ArrayList<String> tempList = new ArrayList<String>();
                for (int i = 0; i < predefinedtagvaluesamount; i++) {
                    tempList.add(new RandomByteIterator(taglengthgenerator.nextLong()).toString());
                }
                if (tagvalues.containsKey(tag)) {
                    throw new WorkloadException("Tagnames contains double value, this should not happen.");
                }
                else {
                    tagvalues.put(tag, tempList);
                }

            }
            if ( !predefinedtagstoreused ) {
                if (!predefinedtagstoragefile.exists() && !predefinedtagstoragefile.isDirectory() && !predefinedtagstoragefile.isFile()) {
                    try {
                        FileOutputStream fileOut = new FileOutputStream(predefinedtagstoragefile);
                        ObjectOutputStream oos = new ObjectOutputStream(fileOut);
                        oos.writeObject(tagvalues);
                    }
                    catch (Exception e) {
                        System.err.println("ERROR: Can't write tagvalues to predefinedtagsotragefile " + predefinedtagstoragefile.getAbsolutePath() + ".");
                        e.printStackTrace();
                    }
                }
                else {
                    System.err.println("ERROR: Can't write tagvalues to predefinedtagsotragefile " + predefinedtagstoragefile.getAbsolutePath() + ".");
                }
            }
        }

        keysequence = new CounterGenerator(insertstart);
        operationchooser = new DiscreteGenerator();
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        if (scanproportion > 0) {
            operationchooser.addValue(scanproportion, "SCAN");
        }

        if (avgproportion > 0) {
            operationchooser.addValue(avgproportion, "AVG");
        }

        if (countproportion > 0) {
            operationchooser.addValue(countproportion, "COUNT");
        }

        if (updateproportion > 0) {
            operationchooser.addValue(updateproportion, "UPDATE");
        }
        transactioninsertkeysequence  = new AcknowledgedCounterGenerator((insertend-insertstart));
        if (sumproportion > 0) {
            operationchooser.addValue(sumproportion, "SUM");
        }
        if (requestdistrib.compareTo("uniform") == 0) {
            keychooser = new UniformLongGenerator(0, (insertend-insertstart)-1);
        }
        else if (requestdistrib.compareTo("latest")==0)
        {
            keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
        }else if(requestdistrib.compareTo("zipfian")==0){
            keychooser = new ZipfianGenerator(0, insertend - insertstart);
        }else {
            throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
        }

        fieldchooser = new UniformLongGenerator(0, tagcount - 1);

        if (scanlengthdistrib.compareTo("uniform") == 0) {
            scanlength = new UniformLongGenerator(minscanlength, maxscanlength);
        }
        else if (scanlengthdistrib.compareTo("zipfian") == 0) {
            scanlength = new ZipfianGenerator(minscanlength, maxscanlength);
        }
        else {
            throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
        }
    }

    private int getRandomRangeInt(Random rand, int min, int max) {
        return rand.nextInt((max - min) + 1) + min;
    }
    private long getRandomRangeLong(Random rand, long min, long max) {
        return min+(long)(Utils.random().nextDouble()*((max - min) + 1));
    }

    private double getRandomDouble(Random rand, int min, int max) {
        return (double) min + (double) (max - min) * rand.nextDouble();
    }

    /**
     * Builds values for all tags.
     */
    private HashMap<String, ByteIterator> buildValues(String key) {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        ArrayList<String> indices = new ArrayList<String>();
        if (randomtagamount) {
            for (int i = 0; i < getRandomRangeInt(rand, 1, this.tagcount); i++) {
                int index = getRandomRangeInt(rand, 0, this.tagcount - 1);
                while (indices.contains(index)) {
                    index = getRandomRangeInt(rand, 0, this.tagcount - 1);
                }
                indices.add(tagnames.get(index));
            }
        }
        else {
            for (int i = 0; i < tagnames.size(); i++) {;
                indices.add(tagnames.get(i));
            }
        }
        String[] tmpArr = new String[this.tagcount];
        if (readfromcsv) {
            if (this.tscr.hasNext()) {
                tmpArr = this.tscr.next();
            }
            else {
                System.err.println("ERROR: CSV file has no more lines.");
             }
        }
        for (String index : indices) {
            ByteIterator data;
            if (predefinedtagvalues && ! readfromcsv) {
                // chose random one predefined tag value
                int randomValIndex = getRandomRangeInt(rand, 0, tagvalues.get(index).size() - 1);
                data = new StringByteIterator(tagvalues.get(index).get(randomValIndex));
                if (predefinedtagstoreused) {
                    try {
                        tmpArr[Integer.valueOf(index.replace(tagPrefix, ""))] = tagvalues.get(index).get(randomValIndex);
                    }
                    catch (NumberFormatException e){
                        System.err.println("ERROR: Can't convert " + index + " to Integer. Only use a-z and A-Z in tag prefix!");
                        e.printStackTrace();
                    }
                }
            }
            else if (readfromcsv) {
                data = new StringByteIterator(tmpArr[Integer.valueOf(index.replace(tagPrefix, ""))]);
            }
            else {
                //fill with random data
                data = new RandomByteIterator(taglengthgenerator.nextLong());
            }
            values.put(index, data);
        }
        if (predefinedtagstoreused) {
            if (usedTags == null) {
                usedTags = new ArrayList<String[]>();
            }
            usedTags.add(tmpArr);
        }
        return values;
    }

    private void generateSearchTagHashmap(HashMap<String, ArrayList<String>> tags) {
        if (this.predefinedtagvalues) {
            if (! predefinedtagstoreused) {
                ArrayList<String> chosenTags = new ArrayList<String>();
                for (int i = 0; i < this.getRandomRangeInt(rand, 1, this.tagcount); i++) {
                    String tag = "";
                    try {
                        // Chose an not yet used tagname (like TAG0 for example)
                        tag = this.tagnames.get(this.getRandomRangeInt(rand, 0, this.tagcount - 1));
                        while (chosenTags.contains(tag)) {
                            tag = this.tagnames.get(this.getRandomRangeInt(rand, 0, this.tagcount - 1));
                        }
                    }
                    catch (Exception e) {
                        System.err.println("ERROR: Error while searching for tagnames. i = " + i);
                    }
                    // Now choose an random (between 1 and querymaxtagvalues) amount of tagvalues
                    ArrayList<String> chosenTagValues = new ArrayList<String>();
                    for (int j = 0; j < this.getRandomRangeInt(rand, 1, this.querymaxtagvalues); j++) {
                        try {
                            String tagValue = this.tagvalues.get(tag).get(this.getRandomRangeInt(rand, 0, this.tagvalues.get(tag).size() - 1));
                            while (chosenTagValues.contains(tagValue)) {
                                tagValue = this.tagvalues.get(tag).get(this.getRandomRangeInt(rand, 0, this.tagvalues.get(tag).size() - 1));
                            }
                            chosenTagValues.add(tagValue);
                        }
                        catch (Exception e) {
                            System.err.println("ERROR: Error while searching for tagvalues. tag = " + tag + " j = " + j);
                        }
                    }
                    tags.put(tag, chosenTagValues);
                }
            }
            else {
                HashMap<String, ArrayList<String>> chosenTags = new HashMap<String, ArrayList<String>>();
                ArrayList<Integer> chosenIndices = new ArrayList<Integer>();
                // Choose how many tagrows you want to use
                for (int i = 0; i < this.getRandomRangeInt(rand, 1, this.querymaxtagvalues); i++) {
                    // Choose Index
                    int index = 0;
                    try {
                        // Chose an not yet used index
                        index = this.getRandomRangeInt(rand, 0, usedTags.size() - 1);
                        while (chosenIndices.contains(index)) {
                            index = this.getRandomRangeInt(rand, 0, usedTags.size() - 1);
                        }
                    }
                    catch (Exception e) {
                        System.err.println("ERROR: Error while searching for index. i = " + i);
                    }
                    // Chose which tags are available
                    ArrayList<Integer> availTags = new ArrayList<Integer>();
                    for (int j = 0; j < usedTags.get(index).length -1; j++) {
                        if (usedTags.get(index)[j] != null && ! usedTags.get(index)[j].equals("")) {
                            availTags.add(j);
                        }
                    }
                    int amount = this.getRandomRangeInt(rand, 1, availTags.size());
                    for (int j = 0; j < amount; j++) {
                        // Take random tag and remove it
                        int tagIndex = this.getRandomRangeInt(rand, 0, availTags.size()-1);
                        if (! chosenTags.containsKey(tagPrefix+availTags.get(tagIndex))) {
                            chosenTags.put(tagPrefix+availTags.get(tagIndex),new ArrayList<String>());
                        }
                        chosenTags.get(tagPrefix+availTags.get(tagIndex)).add(usedTags.get(index)[availTags.get(tagIndex)]);
                        availTags.remove(tagIndex);
                    }
                }
                // Finally put them into tags array
                for (Map.Entry e : chosenTags.entrySet()) {
                    tags.put((String) e.getKey(), (ArrayList<String>) e.getValue());
                }
            }
        }
    }

    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations.
     */
    public boolean doInsert(DB db, Object threadstate) {
        // Check if we alrefy floorfilled or not, if not: make sure theres at least one record per timestamp
        // only let go with possible zero records per timestamp if tsrandom and no floorfilling
        if ( floorcounter >= (insertend-insertstart) ||
                (this.randomamountperts.compareTo("tsrandom") == 0 && ! this.randomfloorfilling) ) {
            if (this.randomamountperts.compareTo("tsrandom") != 0) {
                if (maxrecordcounter < maxrecordspertcounter) {
                    maxrecordcounter++;
                }
                else {
                    maxrecordcounter = 1; // must be 1 because we already use it in this 'round'
                    if (this.randomamountperts.compareTo("random") == 0) {
                        this.maxrecordspertcounter = this.getRandomRangeInt(rand, 1, this.maxrecordsperts);
                    }
                    if (keynum >= insertend-1) {
                        // if we have random amount of records per ts
                        // and we don't make it in after floorfilling + first random amount 'round'
                        // we need to do another random amount 'round'
                        keysequence = new CounterGenerator(insertstart);
                    }
                    keynum = keysequence.nextLong();
                }
            }
            else {
                keynum = this.getRandomRangeLong(rand, this.insertstart, this.insertend-1);
            }
        }
        else {
            keynum = keysequence.nextLong();
            floorcounter++;
            if (floorcounter >= (insertend-insertstart)) {
                keysequence = new CounterGenerator(insertstart);
            }
        }
        // After flooring fill with records left
        //choose the next key
        HashMap<String, ByteIterator> values = buildValues(String.valueOf(keynum));
        return db.insert(metric, new Timestamp(keynum*timeResolution), getRandomDouble(rand, this.valuemin, this.valuemax), values) == 0;
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations.
     */
    public boolean doTransaction(DB db, Object threadstate) {
        String op = operationchooser.nextString();

        if (op.compareTo("READ") == 0) {
            doTransactionRead(db);
        }
        else if (op.compareTo("INSERT") == 0) {
            doTransactionInsert(db);
        }
        else if (op.compareTo("SCAN") == 0) {
            doTransactionScan(db, false, false, false);
        }
        else if (op.compareTo("AVG") == 0) {
            doTransactionScan(db, true, false, false);
        }
        else if (op.compareTo("COUNT") == 0) {
            doTransactionScan(db, false, true, false);
        }
        else if (op.compareTo("SUM") == 0) {
            doTransactionScan(db, false, false, true);
        }
        else if (op.compareTo("UPDATE") == 0){
            doTransactionUpdate(db);
        }

        return true;
    }

    long nextKeynum() {
        // Only LOAD Inserts should be affected by more than one record per timestamp
        // Dropping this code here going back to "normal"
        // It can happen that there are Zero values when using randomamountperts=tsrandom without floorfilling
//         if (this.randomamountperts.compareTo("tsrandom") != 0) {
//         do {
//             keynum2 = insertstart + keychooser.nextLong();
//         }
//         while (keynum2 < insertstart && keynum2 >= insertend);
//         }
//        else {
//            keynum2 = this.getRandomRangeLong(rand, this.insertstart, this.insertend-1);
//        }
        keynum2 = insertstart + keychooser.nextLong();
        return keynum2;
    }

    public void doTransactionRead(DB db) {
        //choose a random key
        long keynum = nextKeynum();
        HashMap<String, ArrayList<String>> tags = new HashMap<String, ArrayList<String>>();
        if (! notagsforread) {
            generateSearchTagHashmap(tags);
        }
        db.read(metric, new Timestamp(keynum*timeResolution), tags);
    }

    public void doTransactionScan(DB db, boolean avg, boolean count, boolean sum) {
        //choose random keys
        long keynum = 0;
        //choose a random scan length
        long len = scanlength.nextLong();
        if (insertstart+len == insertend) {
            keynum = insertstart;
        }
        else {
            keynum = nextKeynum();
            while ((keynum + len) > insertend) {
                keynum = nextKeynum();
            }
        }
        HashMap<String, ArrayList<String>> tags = new HashMap<String, ArrayList<String>>();
        generateSearchTagHashmap(tags);

        db.scan(metric, new Timestamp(keynum*timeResolution), new Timestamp(((keynum+len)*timeResolution)), tags, avg, count , sum ,timevalue, timeunit);
    }

    public void doTransactionInsert(DB db) {
        try {
        //choose a random key
        long keynum = transactioninsertkeysequence.nextLong();
        //choose the next key
        String dbkey = String.valueOf(keynum);
        HashMap<String, ByteIterator> values = buildValues(dbkey);
        db.insert(metric, new Timestamp(keynum*timeResolution), getRandomDouble(rand, this.valuemin, this.valuemax), values);
        //System.err.println("WARNING: INSERT inside RUN phase is untested!.");
        } finally {
            // Don't know if this works (change introduced in 0.4.0
            // if not, just drop "try {" and the finally block and change
            // transactioninsertkeysequence back to CounterGenerator
            // Could have a race condition when going back to 0.2.0 code, see git commits of YCSB
            transactioninsertkeysequence.acknowledge(keynum);
        }
    }

    public void doTransactionUpdate(DB db) {
        //choose a random key
        long keynum = nextKeynum();
        String dbkey = String.valueOf(keynum);
        HashMap<String, ByteIterator> values = buildValues(dbkey);
        db.update(metric, new Timestamp(keynum*timeResolution), getRandomDouble(rand, this.valuemin, this.valuemax), values);
    }

}
