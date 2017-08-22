/*

Package comment should be here. package description.

*/

package main

import (
	as "github.com/aerospike/aerospike-client-go"
    . "github.com/sud82/aerospike-data-sync/logger"
    "bytes"
    "errors"
    "flag"
    "fmt"
    "log"
    "os"
    "strings"
    "strconv"
    "time"
)

// command line arguments
var (
    // External command-line option
    // Server host, user, pass options
    SrcHost string = ""
    SrcUser string = ""
    SrcPass string = ""
    DstHost string = ""
    DstUser string = ""
    DstPass string = ""

    // Scan priority options
    PriorityInt int      = 0

    // AS db related options
    Namespace string  = ""
    Set string        = ""
    BinString string  = ""

    // other cli related options
    UnsyncRecInfoDir  string = ""
    ShowUsage bool    = false
    FindOnly bool     = false
    RemoveFiles bool  = false

    // TODO: Options specific to sync data
    SyncDelete bool   = false
    SyncOnly bool     = false
    UseXdr bool       = false
    UseCksm bool      = false
    // Tps option to throttle sync writes
    Tps int = 0

    // (After-before) timerange options
    ModBeforeString string = ""
    ModAfterString string  = ""

    SamplePer int = 0
    SampleSz int  = 1000

    // logger *log.Logger
    LogLevel int = 1

    /*
    // Tls
    tlsEnable bool = false
    tlsProtocols string = "TLSv1.2"
    tlsCipherSuite bool = true
    tlsRevoke bool = true
    tlsEncryptOnly bool = false
    */

    // Internal options (some external options are changed in these internal
    // options and then used)
    // Number of threads run in parallel to compare src records with dst records
    FindSyncThread = 10
    Priority as.Priority = as.DEFAULT
    BinList []string  = nil
    ModBefore int64        = time.Now().In(time.UTC).UnixNano()
    ModAfter int64         = 0

    UnsyncRecInfoFile string = ""
    UnsyncRecInfoFileCount int = 1
)




// other variables
var (
    srcClientPolicy *as.ClientPolicy = nil
    dstClientPolicy *as.ClientPolicy = nil
    queryPolicy *as.QueryPolicy      = nil

    err error = nil
)


func main() {
    initLogger()

    Logger.Info("****** Starting Aerospike data synchronizer ******")

    flag.StringVar(&SrcHost, "sh", SrcHost, "Source host, eg:x.x.x.x:3000\n")
    flag.StringVar(&SrcUser, "su", SrcUser, "Source host User name.\n")
    flag.StringVar(&SrcPass, "sp", SrcPass, "Source host Password.\n")
    flag.StringVar(&DstHost, "dh", DstHost, "Destination host,eg:x.x.x.x:3000\n")
    flag.StringVar(&SrcUser, "du", SrcUser, "Destination host User name.\n")
    flag.StringVar(&SrcPass, "dp", SrcPass, "Destination host Password.\n")
    flag.StringVar(&Namespace, "n", Namespace, "Aerospike Namespace.\n")
    flag.StringVar(&Set, "s", Set, "Aerospike Set name. Default: All Sets in ns.\n")
    flag.StringVar(&BinString, "b", BinString, "Bin list: bin1,bin2,bin3...\n")
    flag.StringVar(&ModBeforeString, "B", ModBeforeString, "Time before which records modified. eg: Jan 2, 2006 at 3:04pm (MST)\n")
    flag.StringVar(&ModAfterString, "A", ModAfterString, "Time after which records modified. eg: Jan 2, 2006 at 3:04pm (MST)\n")
    flag.StringVar(&UnsyncRecInfoDir, "o", UnsyncRecInfoDir, "Output Dir path to log records to be synced.\n")
    flag.IntVar(&PriorityInt, "P", PriorityInt, "The scan psamplePeiriority. 0 (auto), 1(low), 2 (medium), 3 (high). Default: 0.\n")
    flag.IntVar(&SamplePer, "p", SamplePer, "Sample percentage. Default: 0.\n")
    flag.IntVar(&SampleSz, "sz", SampleSz, "Sample size. if sample percentage given, it won't work. Default: 1000.\n")
    flag.IntVar(&FindSyncThread, "st", FindSyncThread, "Find sync thread. Default: 10.\n")
    flag.BoolVar(&RemoveFiles, "r", RemoveFiles, "Remove existing sync log file.")
    flag.IntVar(&LogLevel, "ll", LogLevel, "Set log level, DEBUG(0), INFO(1), WARNING(2), ERR(3), Default: INFO\n")
    /*
    flag.BoolVar(&tlsEnable, "tls", tlsEnable, "Use TLS/SSL sockets\n")
    flag.BoolVar(&tlsProtocols, "tp", "Allow TLS protocols\n
        Values:SSLv3,TLSv1,TLSv1.1,TLSv1.2 separated by comma\n
        Default: TLSv1.2\n")
    flag.BoolVar(&tlsCipherSuite, "tlsCiphers", tlsCipherSuite, "Allow TLS cipher suites\n
        Values:  cipher names defined by JVM separated by comma\n
        Default: null (default cipher list provided by JVM\n")
    flag.BoolVar(&tlsRevoke, "tr", tlsRevoke, "Revoke certificates identified by
        their serial number\n
        Values:  serial numbers separated by comma\n
        Default: null (Do not revoke certificates\n")
    flag.BoolVar(&tlsEncryptOnly, "te", tlsEncryptOnly, "enable TLS encryption
        and disable TLS certificate validation\n")
    */
    flag.BoolVar(&ShowUsage, "u", ShowUsage, "Show usage information.\n")

    // TODO: Option specific to sync data
    //flag.IntVar(&Tps, "t", Tps, "Throttling limit. will throttle server writes if Tps exceed given limit.\n")
    //flag.BoolVar(&SyncDelete, "sd", SyncDelete, "Delete synced data also. Warning (Don't use this in active-active topology.)\n")
    flag.BoolVar(&FindOnly, "fo", FindOnly, "Tool will just find unsynced data. By default: (find and sync)\n")
    flag.BoolVar(&SyncOnly, "so", SyncOnly, "Tool will just sync records using record log file.\n")
    //flag.BoolVar(&UseXdr, "xdr", UseXdr, "Use XDR to ship unsynced records.\n")
    //flag.BoolVar(&UseCksm, "c", UseCksm, "Compare record checksum.\n")
    //flag.BoolVar(&verbose, "v", verbose, "Verbose mode\n")

	readFlags()

    Logger.Info("Src: %s, Dst: %s, Namespace: %s, Set: %s, Binlist: %s, ModAfter: %s, ModBefore: %s, UnsyncRecInfoDir: %s, Priority: %s, SamplePer:%s",
        SrcHost, DstHost, Namespace, Set, BinString, ModAfterString,
        ModBeforeString, UnsyncRecInfoDir, strconv.Itoa(PriorityInt), strconv.Itoa(SamplePer))

    initUnsyncRecInfoDir()

    initPolicies()

	SrcClient, err = getClient(srcClientPolicy, SrcHost)
	PanicOnError(err)

	DstClient, err = getClient(dstClientPolicy, DstHost)
	PanicOnError(err)

    if !SyncOnly {
        FindRecordsNotInSync()
    }
    // TODO: Currently sync disable

    if !FindOnly {
        DoSync()
    }

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

    if SrcHost == "" {
        err = errors.New("SrcHost not given. Please provide host(x.x.x.x:yyyy).")

    } else if DstHost == "" {
        err = errors.New("DstHost not given. Please provide host(x.x.x.x:yyyy).")

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
    //readPolicy = as.NewPolicy()
    // Get only checksum for record from server
    /*
    if UseCksm {
        readPolicy.ChecksumOnly = true;
    }
    */
    queryPolicy = as.NewQueryPolicy()
    queryPolicy.Priority = Priority

    srcClientPolicy = as.NewClientPolicy()
    srcClientPolicy.User = SrcUser
    srcClientPolicy.Password = SrcPass

    dstClientPolicy = as.NewClientPolicy()
    dstClientPolicy.User = DstUser
    dstClientPolicy.Password = DstPass

}


func getClient(policy *as.ClientPolicy, host string) (*as.Client, error) {
    Logger.Info("Connect to host: %s", host)
    hostInfo := strings.Split(host, ":")
    if len(hostInfo) < 2 {
        PanicOnError(errors.New("Wrong host format. it should be (x.x.x.x:yyyy)."))
    }

    ip := hostInfo[0]
    port, err := strconv.Atoi(hostInfo[1])
    PanicOnError(err)
    return  as.NewClientWithPolicyAndHost(policy, as.NewHost(ip, port))
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
        stat.RecNotInSyncTotal = stat.RecNotInSyncUpdated + stat.RecNotInSyncInserted + stat.RecNotInSyncDeleted
        unsyncStr = "(" + strconv.Itoa(stat.RecNotInSyncTotal)  + "," +
                        strconv.Itoa(stat.RecNotInSyncUpdated)  + "," +
                        strconv.Itoa(stat.RecNotInSyncInserted) + "," +
                        strconv.Itoa(stat.RecNotInSyncDeleted)  + ")"
    }

    // Print "sync(Total, Updated, Inserted, Deleted, GenErr)"
    if !FindOnly {
        stat.RecSyncedTotal = stat.RecSyncedUpdated + stat.RecSyncedInserted + stat.RecSyncedDeleted
        syncStr = "(" + strconv.Itoa(stat.RecSyncedTotal)    + "," +
                        strconv.Itoa(stat.RecSyncedUpdated)  + "," +
                        strconv.Itoa(stat.RecSyncedInserted) + "," +
                        strconv.Itoa(stat.RecSyncedDeleted)  + "," +
                        strconv.Itoa(stat.GenErr) + ")"
    }
    printLine(setStatsMeta, unsyncStr, syncStr)

}


func calcGlobalStat(gSt *TStats, setSts map[string]*TStats) {
    for _, statsObj := range setSts {
        gSt.NObj += statsObj.NObj
        gSt.NScanObj += statsObj.NScanObj

        if !SyncOnly {
            gSt.RecNotInSyncUpdated += statsObj.RecNotInSyncUpdated
            gSt.RecNotInSyncInserted += statsObj.RecNotInSyncInserted
            gSt.RecNotInSyncDeleted += statsObj.RecNotInSyncDeleted
        }
        /*
        if !FindOnly {
            gSt.RecSyncedUpdated += statsObj.RecSyncedUpdated
            gSt.RecSyncedInserted += statsObj.RecSyncedInserted
            gSt.RecSyncedDeleted += statsObj.RecSyncedDeleted
            gSt.GenErr += statsObj.GenErr
        }*/
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

    calcGlobalStat(&GStat, SetStats)

    for setname, statsObj := range SetStats {
        printStat(Namespace, setname, statsObj)
    }

    fmt.Println("\n****** Global stats ******\n")
    printStat(Namespace, "", &GStat)
    fmt.Println()
}

