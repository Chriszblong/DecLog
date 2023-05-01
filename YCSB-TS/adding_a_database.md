# Adding a New Database
I am using IntelliJ IDEA as IDE, you can also use Eclipse or whatever you like. But depending on what you use, you might have to do more mvn steps on your own!)

0. Read https://github.com/brianfrankcooper/YCSB/wiki/Adding-a-Database
1. Add a new folder for the database and a src folder inside of it
    * e.g. opentsdb and opentsdb/src
2. Copy the pom.xml from an existing tsdbfolder and adapt it to the
3. Add the dependencies of the new tsdb to the pom.xml
4. Add a .java file to src/main/java/com/yahoo/ycsb/db/ named NewtsdbClient.java
    * e.g. src/main/java/com/yahoo/ycsb/db/opentsdb/OpenTSDBClient.java
5. Your new class implements DB and needs to implement the following functions:

    ```
    public void init() throws DBException {}
    public void cleanup() throws DBException {}
    public int read(String metric, Timestamp timestamp, HashMap<String, ArrayList<String>> tags) {}
    public int scan(String metric, Timestamp startTs, Timestamp endTs, HashMap<String,ArrayList<String>> tags, boolean avg, boolean count, boolean sum, int timeValue, TimeUnit timeUnit ) {}
    public int insert(String metric, Timestamp timestamp, double value, HashMap<String, ByteIterator> tags) {}
    ```
    * The methods are explained in core/src/main/java/com/yahoo/ycsb/DB.java
6. add your new tsdb client to bin/ycsb
7. run `mvn clean package`
    * look for errors!

## Testing a new TSDB binding

Testing is quite complicated as the new binding needs to go to a generator vm everytime.
At least load and run phase should be tested against testworkloada and testworkloadb. To see if greater workloads do work, run once with a bigger one after everything else works (e.g. workloada).
You need a copy of Vagrant's private SSH Key somewhere. (get it here https://github.com/mitchellh/vagrant/blob/master/keys/vagrant)

1. Create vms for your new tsdb
    * run TSDBBench.py with "-n -o" and without "-w <someworkload>"
    * afterwards you have two vms in your tmpfolder that you can connect to with "vagrant ssh"
2. Go into generator_0 folder and get its ip adress
    ```bash
    vagrant ssh -c "sudo ifconfig"
    ```
3. Get the database IP, go to the database VM folder
    ```bash
    vagrant ssh -c "sudo ifconfig"```
4. Copy your newly packaged yscb to generator
    ```bash
    scp -i /path/to/vagrant/key distribution/target/ycsb-0.4.0.tar.gz vagrant@<generatorIP>:
    ```
5. Connect with Vagrant to generator_0 VM and delete old ycsb and unpack the new one:
    ```bash
    vagrant ssh
    rm -r ycsb
    tar -xvzf ycsb-0.4.0.tar.gz
    mv ycsb-0.4.0 ycsb
    ```
6. Run the LOAD phase of the testworkload: `ycsb/bin/ycsb -jvm-args='-Xmx4096m' load newtsdb -P ycsb/workloads/testworkloadb <other arguments> -p timeseries.granularity=1000 -p histogram.buckets=10 &> load.log`
    * for <other arguments> see the python file of your new tsdb what you have defined (normally at least ip+port)
    * look at load.log and look for errors
    * if predefinedtags.txt, delete it before running LOAD phase again! (if you run it more than once)
7. Run the RUN phase of the testworkload:
        `ycsb/bin/ycsb -jvm-args='-Xmx4096m' run newtsdb -P ycsb/workloads/testworkloadb <other arguments> -p timeseries.granularity=1000 -p histogram.buckets=10 &> run.log`
    * for <other arguments> see the python file of your new tsdb what you have defined (normally at least ip+port)
    * look at run.log and look for errors
8. If you found errors:
    * rebuild ycsb: `mvn clean package`
    * do not copy the whole ycsb-0.4.0.tar.gz, just copy the binding
    * you only need to copy the whole if you changed dependencies!
        *`scp -i /path/to/vagrant/key newtsdb/target/newtsdb-binding-0.4.0.jar  vagrant@<generatorIP>:ycsb/newtsdb-binding/lib/`
  * rerun RUN and LOAD phase
    * if you often only need to test RUN phase, you normally would need a LOAD phase before it everytime
       * the problem are predefined tags in predefinedtags.txt
       * you can copy that file after a LOAD phase with "cp predefinedtags.txt predefinedtags2.txt" once
       * you can than run RUN as often as you need:`cp predefinedtags2.txt predefinedtags.txt && ycsb/bin/ycsb -jvm-args='-Xmx4096m' run newtsdb -P ycsb/workloads/testworkloadb <other arguments> -p timeseries.granularity=1000 -p histogram.buckets=10 &> run.log`

[Back to README.md](../../README.md)