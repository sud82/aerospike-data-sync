/*

Package comment should be here. package description.

*/

package main

import (
	as "github.com/aerospike/aerospike-client-go"
    . "github.com/sud82/aerospike-data-sync/logger"
    "bytes"
    "crypto/tls"
    "errors"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "os"
    "strings"
    "strconv"
    "time"
)


type Cluster struct {
    Host string
    User string
    Pass string
    TLSName string
}

const (
    DEFAULT_TIMEOUT     = 0
    DEFAULT_MAX_RETRIES = 2
    DEFAULT_SAMPLE_SIZE = 1000
)

// command line arguments
var (
    // External command-line option
    SrcCluster Cluster
    DstCluster Cluster

    // AS db related options
    Namespace string  = ""
    Set string        = ""
    BinString string  = ""
    // (After-before) timerange options
    ModBeforeString string = ""
    ModAfterString string  = ""

    UnsyncRecInfoDir  string = ""
    // Scan priority options
    PriorityInt int = 0

    SamplePer int   = 0
    SampleSz int    = DEFAULT_SAMPLE_SIZE

    Timeout int = 0

    // Number of threads run in parallel to compare src records with dst records
    FindSyncThread = 10
    DoSyncThread = 10

    RemoveFiles bool  = false

    // other cli related options
    FindOnly bool     = true
    // logger *log.Logger
    LogLevel int = 1
    // TLS
    TLSConfigFile = ""

    ShowUsage bool    = false

    // TODO: Options specific to sync data
    SyncDelete bool   = false
    SyncOnly bool     = false
    UseXdr bool       = false
    UseCksm bool      = false
    // Tps option to throttle sync writes
    Tps int = 0

    // Internal options (some external options are changed in these internal
    // options and then used)
    Priority as.Priority = as.DEFAULT
    BinList []string  = nil
    ModBefore int64        = time.Now().In(time.UTC).UnixNano()
    ModAfter int64         = 0
    MaxRetries int  = DEFAULT_MAX_RETRIES
    UnsyncRecInfoFile string = ""
    UnsyncRecInfoFileCount int = 1
)




// other variables
var (
    QueryPolicy *as.QueryPolicy = as.NewQueryPolicy()
    ReadPolicy *as.BasePolicy = as.NewPolicy()
    err error = nil
)


func main() {
    initLogger()

    Logger.Info("****** Starting Aerospike data synchronizer ******")

    flag.StringVar(&SrcCluster.Host, "sh", SrcCluster.Host, "Source host, eg:x.x.x.x:3000, x.x.x.x:tls-name:3000\n")
    flag.StringVar(&SrcCluster.User, "su", SrcCluster.User, "Source host User name.\n")
    flag.StringVar(&SrcCluster.Pass, "sp", SrcCluster.Pass, "Source host Password.\n")
    flag.StringVar(&DstCluster.Host, "dh", DstCluster.Host, "Destination host, eg:x.x.x.x:3000, x.x.x.x:tls-name:3000\n")
    flag.StringVar(&DstCluster.User, "du", DstCluster.User, "Destination host User name.\n")
    flag.StringVar(&DstCluster.Pass, "dp", DstCluster.Pass, "Destination host Password.\n")

    flag.StringVar(&Namespace, "n", Namespace, "Aerospike Namespace.\n")
    flag.StringVar(&Set, "s", Set, "Aerospike Set name. Default: All Sets in ns.\n")
    flag.StringVar(&BinString, "b", BinString, "Bin list: bin1,bin2,bin3...\n")
    flag.StringVar(&ModBeforeString, "B", ModBeforeString, "Time before which records modified. eg: Jan 2, 2006 at 3:04pm (MST)\n")
    flag.StringVar(&ModAfterString, "A", ModAfterString, "Time after which records modified. eg: Jan 2, 2006 at 3:04pm (MST)\n")
    flag.StringVar(&UnsyncRecInfoDir, "o", UnsyncRecInfoDir, "Output Dir path to log records to be synced.\n")
    flag.IntVar(&PriorityInt, "P", PriorityInt, "The scan Priority. 0 (auto), 1(low), 2 (medium), 3 (high). Default: 0.\n")
    flag.IntVar(&SamplePer, "p", SamplePer, "Sample percentage. Default: 0.\n")
    flag.IntVar(&SampleSz, "ss", SampleSz, "Sample size. if sample percentage given, it won't work. Default: 1000.\n")
    flag.IntVar(&Timeout, "T", Timeout, "Set read and write transaction timeout in milliseconds.. Default: 0.\n")
    //flag.IntVar(&MaxRetries, "mr", MaxRetries, "Maximum number of retries before aborting the current transaction. Default: 2.\n")
    flag.IntVar(&FindSyncThread, "fst", FindSyncThread, "Find sync thread. Parallel request to server. Default: 10.\n")
    flag.IntVar(&DoSyncThread, "dst", FindSyncThread, "Do sync thread. Parallel request to server. Default: 10.\n")
    flag.BoolVar(&RemoveFiles, "r", RemoveFiles, "Remove existing sync log file.\n")
    flag.IntVar(&LogLevel, "ll", LogLevel, "Set log level, DEBUG(0), INFO(1), WARNING(2), ERR(3), Default: INFO\n")
    flag.StringVar(&TLSConfigFile, "tcf", TLSConfigFile, "TLS config filepath.\n")

    flag.BoolVar(&ShowUsage, "u", ShowUsage, "Show usage information.\n")

    // TODO: Option specific to sync data
    //flag.IntVar(&Tps, "t", Tps, "Throttling limit. will throttle server writes if Tps exceed given limit.\n")
    //flag.BoolVar(&SyncDelete, "sd", SyncDelete, "Delete synced data also. Warning (Don't use this in active-active topology.)\n")
    //flag.BoolVar(&FindOnly, "fo", FindOnly, "Tool will just find unsynced data. By default: (find and sync)\n")
    //flag.BoolVar(&SyncOnly, "so", SyncOnly, "Tool will just sync records using record log file.\n")
    //flag.BoolVar(&UseXdr, "xdr", UseXdr, "Use XDR to ship unsynced records.\n")
    //flag.BoolVar(&UseCksm, "c", UseCksm, "Compare record checksum.\n")
    //flag.BoolVar(&verbose, "v", verbose, "Verbose mode\n")

	readFlags()

    Logger.Info("Src: %s, Dst: %s, Namespace: %s, Set: %s, Binlist: %s,
    ModAfter: %s, ModBefore: %s, UnsyncRecInfoDir: %s, Priority: %s, SamplePer:%s, SampleSz:%s",
        SrcCluster.Host, DstCluster.Host, Namespace, Set, BinString, ModAfterString,
        ModBeforeString, UnsyncRecInfoDir, strconv.Itoa(PriorityInt), strconv.Itoa(SamplePer), strconv.Itoa(SampleSz))

    initUnsyncRecInfoDir()

    initPolicies()

	SrcClient, err = getClient(&SrcCluster)
	PanicOnError(err)

	DstClient, err = getClient(&DstCluster)
	PanicOnError(err)

    if !SyncOnly {
        FindRecordsNotInSync()
    }
    // TODO: Currently sync disable
    /*
    if !FindOnly {
        DoSync()
    }
    */
    printAllStats()
}


func initLogger() {
	var buf bytes.Buffer
	logger := log.New(&buf, "", log.LstdFlags|log.Lshortfile)
    logger.SetOutput(os.Stdout)

    // Init log file to direct logs to the file
    os.MkdirAll("log", os.ModePerm)
    logfile, err := os.OpenFile("log/sync.log",os.O_CREATE|os.O_WRONLY|os.O_APPEND,os.ModePerm)
    if err == nil {
        logger.SetOutput(logfile)
    } else {
        logger.Print("Failed to log to file, using default stderr")
    }

    // Set customzed logger
    Logger.SetLogger(logger)
	Logger.SetLevel(INFO)
}


func readFlags() {
    Logger.Info("Parsing input arguments.")
    flag.Parse()

    if ShowUsage {
        fmt.Println("********** Usage **********")
        flag.Usage()
        os.Exit(0)
    }

    if SrcCluster.Host == "" {
        err = errors.New("SrcCluster.Host not given. Please provide host(x.x.x.x:yyyy).")

    } else if DstCluster.Host == "" {
        err = errors.New("DstCluster.Host not given. Please provide host(x.x.x.x:yyyy).")

    } else if Namespace == "" {
        err = errors.New("Namespace not given. Please provide Namespace.")

    }

    if BinString != "" {
        // Replace all whitespaces
        BinString = strings.Replace(BinString," ", "", -1 )
        BinList = strings.Split(BinString, ",")
    }

    if ModAfterString != "" {
        ModAfter = timeStringToTimestamp(ModAfterString)
    }

    if ModBeforeString != "" {
        ModBefore = timeStringToTimestamp(ModBeforeString)
    }

    if ModBefore < ModAfter {
        err = errors.New("Timerange incorrect. modafter > modbefore.")
    }

    // TODO: should we add src, dst host param also in config this file
    if TLSConfigFile != "" {
        InitTLSConfig(TLSConfigFile)

    }

    PanicOnError(err)

    // Scan priorities
    if PriorityInt != 0 {
        if PriorityInt == 1 {
            Priority = as.LOW
        } else if PriorityInt == 2 {
            Priority = as.MEDIUM
        } else if PriorityInt == 3 {
            Priority = as.HIGH
        }
        Logger.Info("Scan Priority set to: " + strconv.Itoa(PriorityInt))
    }

    // Set log level
    if LogLevel != 1 {
        if LogLevel == 0 {
            Logger.SetLevel(DEBUG)
        } else if LogLevel == 2 {
            Logger.SetLevel(WARNING)
        } else if LogLevel == 3 {
            Logger.SetLevel(ERR)
        }
    }
}


func initUnsyncRecInfoDir() {
    Logger.Info("Init Unsync_record_info Dir: %s", UnsyncRecInfoDir)
    if UnsyncRecInfoDir == "" {
        if !FindOnly {
            err = errors.New("Unsync_record_info Dir path required.")
        } else {
            Logger.Debug("No path for Unsync_record_info Dir. Returning without initialization. ")
            return
        }
    }

    if _, err := os.Stat(UnsyncRecInfoDir); !os.IsNotExist(err) && !SyncOnly {
        if RemoveFiles {
            Logger.Info("Remove old Unsync_record_info Dir: %s", UnsyncRecInfoDir)
            os.RemoveAll(UnsyncRecInfoDir)
        } else {
            PanicOnError(errors.New("Unsync_record_info Dir already exist. Use -r or Please remove it: " + UnsyncRecInfoDir))
        }
    }

    // Create Dir if not synconly
    if !SyncOnly {
        // create and write header in file
        Logger.Info("Create new Unsync_record_info Dir: %s", UnsyncRecInfoDir)
        err = os.MkdirAll(UnsyncRecInfoDir, os.ModePerm)
        PanicOnError(err)
        InitUnsyncRecInfoFile()
    }
}


// Init scan, clinet policies
func initPolicies() {
    Logger.Info("Init all client, query policies.")
    ReadPolicy.Timeout = time.Duration(Timeout) * time.Millisecond
    ReadPolicy.MaxRetries = MaxRetries
    // Get only checksum for record from server
    //ReadPolicy.ChecksumOnly = true
    QueryPolicy.Priority = Priority
}


func getHost(c *Cluster) *as.Host {

    hostInfo := strings.Split(c.Host, ":")
    if len(hostInfo) < 2 {
        PanicOnError(errors.New("Wrong host format. it should be (x.x.x.x:tls_name:yyyy)."))
    }

    ip := hostInfo[0]
    var port int
    if len(hostInfo) == 3 {
        c.TLSName = hostInfo[1]
        port, err = strconv.Atoi(hostInfo[2])
    } else {
        port, err = strconv.Atoi(hostInfo[1])
    }

    PanicOnError(err)
    host := as.NewHost(ip, port)

    if c.TLSName != "" {
        host.TLSName = c.TLSName
    }
    return host
}

func getClientPolicy(c *Cluster) *as.ClientPolicy {
    policy := as.NewClientPolicy()
    policy.User = c.User
    policy.Password = c.Pass

    if c.TLSName != "" || TLSConfig.TLS.EncryptOnly == true {
        // Setup TLS Config
        tlsConfig := &tls.Config{
            Certificates:             TLSConfig.ClientPool(),
            RootCAs:                  TLSConfig.ServerPool(),
            InsecureSkipVerify:       TLSConfig.TLS.EncryptOnly,
            PreferServerCipherSuites: true,
        }
        tlsConfig.BuildNameToCertificate()
        policy.TlsConfig = tlsConfig
    }
    return policy
}


func getClient(c *Cluster) (*as.Client, error) {
    Logger.Info("Connect to host: %s", c.Host)
    host := getHost(c)
    policy := getClientPolicy(c)
    return  as.NewClientWithPolicyAndHost(policy, host)
}


//----------------------------------------------------------------------------
// Other helper functions
//----------------------------------------------------------------------------

// Main stats line printer
func printLine(setStatsMeta []string, unsyncStr string, syncStr string) {
    for _, m := range setStatsMeta {
        fmt.Printf("%30s", m)
    }

    if !SyncOnly {
        fmt.Printf("%60s", unsyncStr)
    }

    if !FindOnly {
        fmt.Printf("%60s", syncStr)
    }
    fmt.Println()
}


// Create set_stats to string to print
func printStat(ns string, set string, stat *TStats) {

    // Header [Namespace, Set, Total_Records, Sampled_Records,
    // Unsync(Total, Updated, Inserted, Deleted),
    // Sync(Total, Updated, Inserted, Deleted, GenErr)]

    // Print ("Namespace", "Set", "Total_Records", "Sampled_Records")
    var setStatsMeta []string
    setStatsMeta = append(setStatsMeta, ns, set, strconv.Itoa(stat.NObj), strconv.Itoa(stat.NScanObj))

    unsyncStr := ""
    syncStr := ""

    // Print "Unsync(Total, Updated, Inserted, Deleted)"
    if !SyncOnly {
        unsyncStr = "(" + strconv.Itoa(stat.RecNotInSyncTotal)  + "," +
                        strconv.Itoa(stat.RecNotInSyncUpdated)  + "," +
                        strconv.Itoa(stat.RecNotInSyncInserted) + "," +
                        strconv.Itoa(stat.RecNotInSyncDeleted)  + ")"
    }

    // Print "sync(Total, Updated, Inserted, Deleted, GenErr)"
    if !FindOnly {
        syncStr = "(" + strconv.Itoa(stat.RecSyncedTotal)    + "," +
                        strconv.Itoa(stat.RecSyncedUpdated)  + "," +
                        strconv.Itoa(stat.RecSyncedInserted) + "," +
                        strconv.Itoa(stat.RecSyncedDeleted)  + "," +
                        strconv.Itoa(stat.DoSync.GenErr) + ")"
    }
    printLine(setStatsMeta, unsyncStr, syncStr)

}


func calcGlobalStat(gSt *TStats, setSts map[string]*TStats) {
    for _, statsObj := range setSts {
        gSt.NObj += statsObj.NObj
        gSt.NScanObj += statsObj.NScanObj
        gSt.NSampleObj += statsObj.NSampleObj

        if !SyncOnly {
            gSt.RecNotInSyncUpdated += statsObj.RecNotInSyncUpdated
            gSt.RecNotInSyncInserted += statsObj.RecNotInSyncInserted
            gSt.RecNotInSyncDeleted += statsObj.RecNotInSyncDeleted
            gSt.FindSync.ScanReqErr += statsObj.FindSync.ScanReqErr
            gSt.FindSync.Err += statsObj.FindSync.Err

            gSt.RecNotInSyncTotal += statsObj.RecNotInSyncTotal
        }

        if !FindOnly {
            gSt.RecSyncedUpdated += statsObj.RecSyncedUpdated
            gSt.RecSyncedInserted += statsObj.RecSyncedInserted
            gSt.RecSyncedDeleted += statsObj.RecSyncedDeleted
            gSt.DoSync.GenErr += statsObj.DoSync.GenErr
            gSt.DoSync.Err += statsObj.DoSync.Err

            gSt.RecSyncedTotal += statsObj.RecSyncedTotal
        }
    }
}


// Print All set stats, global stats
func printAllStats() {

    fmt.Printf("\n\n****** Data Sync Output***\n\n")

    // Print metainfo
    fmt.Println("Modified after: " + timestampToTimeString(ModAfter))
    fmt.Println("Modified before: " + timestampToTimeString(ModBefore))

    fmt.Println("\n****** Set stats *********")

    // Print header
    metaList  := []string{"Namespace", "Set", "Total_Records", "Sampled_Records"}
    unsyncStr := "Unsync(Total, Updated, Inserted, Deleted)"
    syncStr   := "Sync(Total, Updated, Inserted, Deleted, GenErr)"
    printLine(metaList, unsyncStr, syncStr)

    for setname, statsObj := range SetStats {
        statsObj.RecNotInSyncTotal = statsObj.RecNotInSyncUpdated + statsObj.RecNotInSyncInserted + statsObj.RecNotInSyncDeleted
        statsObj.RecSyncedTotal = statsObj.RecSyncedUpdated + statsObj.RecSyncedInserted + statsObj.RecSyncedDeleted
        printStat(Namespace, setname, statsObj)
    }

    fmt.Println("\n****** Global stats ******\n")

    calcGlobalStat(&GStat, SetStats)
    // Log global and per set stats
    g, err := json.Marshal(GStat)
    if err == nil {
        Logger.Info("Global Stat: " + string(g))
    }
    s, err := json.Marshal(SetStats)
    if err == nil {
        Logger.Info("Sets Stat: " + string(s))
    }

    printStat(Namespace, "", &GStat)
    fmt.Println()
}

