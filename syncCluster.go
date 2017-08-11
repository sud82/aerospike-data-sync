/*

Package comment should be here. package description.

*/

package main

import (
	as "github.com/aerospike/aerospike-client-go"
    . "github.com/sud82/aerospike-data-sync/logger"
    "bufio"
    "bytes"
    "encoding/hex"
    "errors"
    "flag"
    "fmt"
    "log"
    "os"
    "reflect"
    "strings"
    "sync"
    "strconv"
    "time"
)


type TStats struct {
    // Total checked objects
    nObj int
    // Num of Sampled objects
    nSampleObj int
    // Track total scanned records
    scanReq int

    // Stats for records found not in sync
    recNotInSyncTotal int
    recNotInSyncUpdated int
    recNotInSyncInserted int
    recNotInSyncDeleted int

    // Stats for records synced
    recSyncedTotal int
    recSyncedUpdated int
    recSyncedInserted int
    recSyncedDeleted int

    // Generation error at (check and write) operation
    genErr int
    // Error returned by server while scaning records
    reqErr int

}


const (
    version string = "1.0"
    insertedOp string = "I"
    updatedOp string  = "U"
    deletedOp string  = "D"
    // Header args separater in sync.log file
    headerDel = "#"
    // Number of args saved in sync.log file per record(recInfo)
    recLineArgs = 4
)


// arguments
var (
    err error = nil

    // Server host, user, pass options
    srcHost string = ""
    srcUser string = ""
    srcPass string = ""

    dstHost string = ""
    dstUser string = ""
    dstPass string = ""

    // Sacn priority options
    priorityInt int      = 0
    priority as.Priority = as.DEFAULT

    // AS db related options
    namespace string     = ""
    set string           = ""
    binString string     = ""
    binList []string     = nil

    // other cli related options
    recLogFile string = ""
    verbose bool      = false
    showUsage bool    = false
    syncDelete bool   = false
    findOnly bool     = false
    syncOnly bool     = false
    useXdr bool       = false
    useCksm bool      = false
    removeFiles bool  = false

    // (After-before) timerange options
    modBefore int64        = time.Now().In(time.UTC).UnixNano()
    modAfter int64         = 0
    modBeforeString string = ""
    modAfterString string  = ""
    // Used by time parser
    timeLayout string      = "Jan 2, 2006 at 3:04pm (MST)"

    // AS client related
    srcClientPolicy *as.ClientPolicy = nil
    dstClientPolicy *as.ClientPolicy = nil

    srcClient *as.Client = nil
    dstClient *as.Client = nil

    readPolicy *as.BasePolicy = nil

    queryPolicy *as.QueryPolicy = nil

    findSyncThread = 1
    // Global stats to track synced, unsynced records
    gStat TStats
    // Track stats for all sets within namespace
    setStats = map[string]*TStats{}

    // For threshold, Time window (track time after 1 sec)
    timeWEnd = time.Now()
    recSyncedTotalOld int = 0

    // Extra data stats, variable
    samplePer int = 10
    tps int = 0

    logger *log.Logger

)

func initLogger() {
	var buf bytes.Buffer
	logger = log.New(&buf, "", log.LstdFlags|log.Lshortfile)
    logger.SetOutput(os.Stdout)
    logger.Print("Init logger.")

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
	Logger.SetLevel(DEBUG)

}

func main() {
    initLogger()

    Logger.Info("****** Starting Aerospike data synchronizer ******")

    flag.StringVar(&srcHost, "sh", srcHost, "Source host, eg:x.x.x.x:3000\n")
    flag.StringVar(&srcUser, "su", srcUser, "Source host User name.\n")
    flag.StringVar(&srcPass, "sp", srcPass, "Source host Password.\n")
    flag.StringVar(&dstHost, "dh", dstHost, "Destination host,eg:x.x.x.x:3000\n")
    flag.StringVar(&srcUser, "du", srcUser, "Destination host User name.\n")
    flag.StringVar(&srcPass, "dp", srcPass, "Destination host Password.\n")
    flag.StringVar(&namespace, "n", namespace, "Aerospike namespace.\n")
    flag.StringVar(&set, "s", set, "Aerospike set name. Default: All sets in ns.\n")
    flag.StringVar(&binString, "B", binString, "Bin list: bin1,bin2,bin3...\n")
    flag.StringVar(&modBeforeString, "b", modBeforeString, "Time before which records modified. eg: Jan 2, 2006 at 3:04pm (MST)\n")
    flag.StringVar(&modAfterString, "a", modAfterString, "Time after which records modified. eg: Jan 2, 2006 at 3:04pm (MST)\n")
    flag.StringVar(&recLogFile, "o", recLogFile, "Output File to log records to be synced.\n")
    //flag.IntVar(&tps, "t", tps, "Throttling limit. will throttle server writes if tps exceed given limit.\n")
    flag.IntVar(&priorityInt, "f", priorityInt, "The scan priority. 0 (auto), 1(low), 2 (medium), 3 (high). Default: 0.\n")
    flag.IntVar(&samplePer, "p", samplePer, "Sample percentage. Default: 10.\n")
    flag.IntVar(&findSyncThread, "st", findSyncThread, "Find sync thread. Default: 10.\n")
    flag.BoolVar(&removeFiles, "r", removeFiles, "Remove existing sync log file.")
    flag.BoolVar(&syncDelete, "sd", syncDelete, "Delete synced data also. Warning (Don't use this in active-active topology.)\n")
    flag.BoolVar(&findOnly, "fo", findOnly, "Tool will just find unsynced data. By default: (find and sync)\n")
    flag.BoolVar(&syncOnly, "so", syncOnly, "Tool will just sync records using record log file.\n")
    //flag.BoolVar(&useXdr, "xdr", useXdr, "Use XDR to ship unsynced records.\n")
    //flag.BoolVar(&useCksm, "c", useCksm, "Compare record checksum.\n")
    flag.BoolVar(&verbose, "v", verbose, "Verbose mode\n")
    flag.BoolVar(&showUsage, "u", showUsage, "Show usage information.\n")

	readFlags()

    Logger.Info("Src: %s, Dst: %s, Namespace: %s, Set: %s, Binlist: %s, ModAfter: %s, ModBefore: %s, RecLogFile: %s, Priority: %s, SamplePer:%s",
    srcHost, dstHost, namespace, set, binString, modAfterString,
    modBeforeString, recLogFile, strconv.Itoa(priorityInt), strconv.Itoa(samplePer))

    initSyncLogFile()

    initPolicies()

	srcClient, err = getClient(srcClientPolicy, srcHost)
	panicOnError(err)

	dstClient, err = getClient(dstClientPolicy, dstHost)
	panicOnError(err)
    if !syncOnly {
        findRecordsNotInSync()
    }
    // TODO: Currently sync disable

    if !findOnly {
        doSync()
    }

    printAllStats()
}


func readFlags() {
    Logger.Info("Parsing input arguments.")
    flag.Parse()

    if showUsage {
        fmt.Println("********** Usage **********")
        flag.Usage()
        os.Exit(0)
    }

    if srcHost == "" {
        err = errors.New("srcHost not given. Please provide host(x.x.x.x:yyyy).")

    } else if dstHost == "" {
        err = errors.New("dstHost not given. Please provide host(x.x.x.x:yyyy).")

    } else if namespace == "" {
        err = errors.New("namespace not given. Please provide namespace.")

    // recLogFile needed if its not findonly.
    } else if recLogFile == "" {
        if !findOnly {
            err = errors.New("Record log file path required.")
        }
    }

    if binString != "" {
        binList = strings.Split(binString, ",")
    }

    if modAfterString != "" {
        modAfter = timeStringToTimestamp(modAfterString)
    }

    if modBeforeString != "" {
        modBefore = timeStringToTimestamp(modBeforeString)
    }

    if modBefore < modAfter {
        err = errors.New("Timerange incorrect. modafter > modbefore.")
    }

    panicOnError(err)
    // Scan priorities
    if priorityInt != 0 {
        if priorityInt == 1 {
            priority = as.LOW
        } else if priorityInt == 2 {
            priority = as.MEDIUM
        } else if priorityInt == 3 {
            priority = as.HIGH
        }
    }
}


func initSyncLogFile() {
    Logger.Info("Init sync log file: %s", recLogFile)
    if recLogFile == "" {
        Logger.Debug("No path for sync file. Returning without initialization. ")
        return
    }

    if _, err := os.Stat(recLogFile); !os.IsNotExist(err) && !syncOnly {
        if removeFiles {
            Logger.Info("Remove old Record log file: %s", recLogFile)
            os.Remove(recLogFile)
        } else {
            panicOnError(errors.New("Record log file already exist. Use -r or Please remove it: " + recLogFile))
        }
    }

    // Create file and write header if not synconly
    if !syncOnly {
        // create and write header in file
        Logger.Info("Create new Record log file: %s", recLogFile)
        file, err := os.OpenFile(recLogFile, os.O_CREATE|os.O_WRONLY, 0600)
        if err != nil {
            panicOnError(err)
        }
        defer file.Close()

        header := getLogFileHeader(version, modAfter, modBefore, namespace, set, binString)
        if _, err = file.WriteString(header); err != nil {
            panicOnError(err)
        }
    }
}


func getLogFileHeader(version string, modAfter int64, modBefore int64, ns string, set string, binString string) string {
    // #ver:1#mod_after:1212334#mod_before:123233#ns:test#set:testset#bins:b1,b2,b3#

    modAfterString := timestampToTimeString(modAfter)
    modBeforeString := timestampToTimeString(modBefore)

    hd := headerDel
    return "version:" + version + hd + "mod_after:" + modAfterString + hd +
    "mod_before:" + modBeforeString + hd + "namespace:" + ns + hd + "bins:" + binString +
    "\n" + "Action" + hd + "Digest" + hd + "Set" + hd + "Gen" + "\n"
}


// Init scan, clinet policies
func initPolicies() {
    Logger.Info("Init all client, query policies.")
    readPolicy = as.NewPolicy()
    // Get only checksum for record from server
    /*
    if useCksm {
        readPolicy.ChecksumOnly = true;
    }
    */

    srcClientPolicy = as.NewClientPolicy()
    srcClientPolicy.User = srcUser
    srcClientPolicy.Password = srcPass

    dstClientPolicy = as.NewClientPolicy()
    dstClientPolicy.User = dstUser
    dstClientPolicy.Password = dstPass

    queryPolicy = as.NewQueryPolicy()
    queryPolicy.Priority = priority
}


func getClient(policy *as.ClientPolicy, host string) (*as.Client, error) {
    Logger.Info("Connect to host: %s", host)
    hostInfo := strings.Split(host, ":")
    if len(hostInfo) < 2 {
        panicOnError(errors.New("Wrong host format. it should be (x.x.x.x:yyyy)."))
    }

    ip := hostInfo[0]
    port, err := strconv.Atoi(hostInfo[1])
    panicOnError(err)
    return  as.NewClientWithPolicyAndHost(policy, as.NewHost(ip, port))
}


//----------------------------------------------------------------------
// Find not in sync func and helpers
//----------------------------------------------------------------------
// Main func to found records not in sync
func findRecordsNotInSync() {
    Logger.Info("Find records not in sync")
    wg := new(sync.WaitGroup)

    var dstRecordset *as.Recordset = nil

    // Channel to store unsync record's info, max 100 record at a time
    recordInfoChan := make(chan string, 100000)

    // Open record log file to write found unsync records
    var file *os.File = nil
    if recLogFile != "" {
        file, err = os.OpenFile(recLogFile, os.O_APPEND|os.O_WRONLY, 0600)
        if err != nil {
            panicOnError(err)
        }
        defer file.Close()
    }

    // Get replication factor
    replFact := getReplicationFact(srcClient, namespace)
    if replFact == 0 {
        panicOnError(errors.New("Coundn't get replication factor for NS: " + namespace + ". check Config."))
    }

    // Parsed sets Stats fetched from source aesospike server
    allSetStatsMap := getSetMap(srcClient, namespace)

    // Scan records from source and validate them by running
    // multiple validation threads 
    for setname, statsMap := range allSetStatsMap {

        if set != "" && set != setname {
            continue
        }

        // It gives all objects so divide by repl factor
        nObj := getObjectCount(statsMap, namespace)
        nObj = nObj / replFact
        if nObj == 0 {
            continue
        }

        // Update set stats
        setStats[setname] = &TStats{}
        setStats[setname].nObj = nObj
        setStats[setname].nSampleObj = nObj * samplePer / 100

        srcRecordset := getRecordset(srcClient, namespace, setname, binList, modAfter, modBefore)

        // Run multiple thread to fetch records from queryRecordQueue
        // and validate those records to see if they are in sync or not
        for w := 0; w < findSyncThread; w++ {
            wg.Add(1)

            go func(setname string) {
                defer wg.Done()

                validateAndFind(srcRecordset, dstRecordset, recordInfoChan, setname)
            }(setname)
        }
    }

    // Wait for completing all threads and closing recordInfoChan
    go func() {
        wg.Wait()
        close(recordInfoChan)
    }()

    // Continue looping if sync log file doesn't exist.
    // This has to wait for closing recordInfoChan
    for r := range recordInfoChan {
        if recLogFile == "" {
            continue
        }
        if _, err = file.WriteString(r); err != nil {
            panicOnError(err)
        }
    }
}


// Validate and find records not in Sync
// Inserted, Updated: Records which are not replicated to destination
// Deleted: Records which are deleted in source but not in destination
// Note: Its not possible to find deleted unsynced records in A-A topology
func validateAndFind(srcRecordset *as.Recordset, dstRecordset *as.Recordset, recordInfoChan chan string, setname string) {
    Logger.Info("Thread to fetch and match src and dst records. SET: %s", setname)
    Logger.Info("Find Updated, Inserted record if not in sync.")
    sStat := setStats[setname]
L1:
	for {

		select {

		case srcRec := <-srcRecordset.Records:
            // Break If scan bucket is giving more then sampled object
			if srcRec == nil || (sStat.scanReq >= sStat.nSampleObj) {
                Logger.Info("Src, Sample limit reached or No record left to match. SET: %s", setname)
                Logger.Info("Src, Scanned records: %s, Sample Size: %s. SET: %s",
                    strconv.Itoa(sStat.scanReq),
                    strconv.Itoa(sStat.nSampleObj), setname)
                break L1
			}
            sStat.scanReq++

            // TODO: Add LUT check for record. LUT. skip if srcRecord.LUT > Timestamp
            dstRec,_ := dstClient.Get(readPolicy, srcRec.Key, binList...)

            //panicOnError(err)
            // If rec doesn't exist in dst, it's new insert in src. log it. Add gen = 0 for new rec
            if dstRec == nil || len(dstRec.Bins) == 0 {
                var gen uint32 = 0
                if dstRec != nil {
                    gen = dstRec.Generation
                }

                recordInfoChan <- getRecordLogInfoLine(insertedOp, srcRec.Key, gen)
                sStat.recNotInSyncInserted++
                gStat.recNotInSyncInserted++
                Logger.Debug("Record op Insert. gStat_RecNotInSync: %s, setStat_RecNotInSync: %s. SET: %s",
                    strconv.Itoa(gStat.recNotInSyncInserted),
                    strconv.Itoa(sStat.recNotInSyncInserted), setname)
                continue
            }

            // src and dst record doesn't match. Record Updated. log it.
            if !reflect.DeepEqual(srcRec.Bins, dstRec.Bins) {
                fmt.Println("src")
                fmt.Println("dst")
                fmt.Println()

                recordInfoChan <- getRecordLogInfoLine(updatedOp, srcRec.Key, dstRec.Generation)
                sStat.recNotInSyncUpdated++
                gStat.recNotInSyncUpdated++
                Logger.Debug("Record op Update. gStat_RecNotInSync: %s, setStat_RecNotInSync: %s. SET: %s",
                    strconv.Itoa(gStat.recNotInSyncUpdated),
                    strconv.Itoa(sStat.recNotInSyncUpdated), setname)
            }

		case err := <-srcRecordset.Errors:
            Logger.Debug("Record read error: %s. SET: %s", err.Error(), setname)
            sStat.reqErr++
			//fmt.Println(err)
            continue
		}
	}

    // Don't go for checking deletes
    if syncDelete == false {
        return
    }
    Logger.Info("Find Deleted record if not in sync. SET: %s", setname)

L2:
	for {
		select {
		case dstRec := <-dstRecordset.Records:
	        // Break If scan bucket is giving more then sampled object
            if (dstRec == nil) {
                Logger.Info("Dst: No record left to match. SET: %s", setname)
				break L2
			}
            sStat.scanReq++

            srcRec, _ := srcClient.Get(readPolicy, dstRec.Key, binList...)
            //panicOnError(err)

            if  srcRec == nil || len(srcRec.Bins) == 0 {
                recordInfoChan <- getRecordLogInfoLine(deletedOp, dstRec.Key, dstRec.Generation)
                sStat.recNotInSyncDeleted++
                gStat.recNotInSyncDeleted++
                Logger.Debug("Record op Delete. gStat_RecNotInSync: %s, setStat_RecNotInSync: %s. SET: %s",
                    strconv.Itoa(gStat.recNotInSyncDeleted),
                    strconv.Itoa(sStat.recNotInSyncDeleted), setname)
                continue
            }

		case err := <-srcRecordset.Errors:
            Logger.Debug("Record read error: %s. SET: %s", err.Error(), setname)
            sStat.reqErr++
			//fmt.Println(err)
            continue
		}
	}
}


// Scan all records in given timerange
func getRecordset(client *as.Client, ns string, set string,  binList []string, modAfter int64, modBefore int64) *as.Recordset {
    Logger.Info("Send query and create RecordSet. NS: %s, SET: %s, BINLIST: %s", ns, set, binList)
    stm := as.NewStatement(ns, set, binList...)

    createTimeRangeStm(stm, modAfter, modBefore)

    recordset, err := client.Query(queryPolicy, stm)

    panicOnError(err)
    return recordset
}


// Create statement with time bound for predex
func createTimeRangeStm(stm *as.Statement, modAfter int64, modBefore int64) {
    if modAfter == 0 {
        stm.SetPredExp(
            as.NewPredExpRecLastUpdate(),
            as.NewPredExpIntegerValue(modBefore),
            as.NewPredExpIntegerLessEq(),
        )
    } else {
        stm.SetPredExp(
            as.NewPredExpRecLastUpdate(),
            as.NewPredExpIntegerValue(modAfter),
            as.NewPredExpIntegerGreater(),
            as.NewPredExpRecLastUpdate(),
            as.NewPredExpIntegerValue(modBefore),
            as.NewPredExpIntegerLessEq(),
            as.NewPredExpAnd(2),
        )
    }
}


// Get replication factor, return 0 if stat not present
func getReplicationFact(client *as.Client, ns string) int {
    for _, node := range client.GetNodes() {
        info, err := requestNodeNamespace(node, ns)
        panicOnError(err)
        if replFact, ok := info["repl-factor"]; ok {
            r, err := strconv.Atoi(replFact)
            panicOnError(err)
            return r
        }
    }
    return 0
}


// RequestNodeStats returns statistics for the specified node as a map
func requestNodeNamespace(node *as.Node, ns string) (map[string]string, error) {
	infoMap, err := as.RequestNodeInfo(node, "namespace/" + ns)
	if err != nil {
		return nil, err
	}

	res := map[string]string{}

	v, exists := infoMap["namespace/" + ns]
	if !exists {
		return res, nil
	}

	values := strings.Split(v, ";")
	for i := range values {
		kv := strings.Split(values[i], "=")
		if len(kv) > 1 {
			res[kv[0]] = kv[1]
		}
	}

	return res, nil
}


// Request set statistics from AS server and parse it to create a map for sets.
// {set: {node1: {setstats...}, node2: {setstats..}}}
func getSetMap(client *as.Client, ns string) map[string]map[string]map[string]string {

    allSets := map[string]map[string]map[string]string{}

    for _, node := range client.GetNodes() {

        // map[sets/:ns=test:set=testset:disable-eviction=false;
        //           ns=test:set=bar:objects=10000;]

        setInfo, err := as.RequestNodeInfo(node, "sets/")
        panicOnError(err)
        setList := strings.Split(setInfo["sets/"], ";")

        for _, setItem := range(setList) {
            setMap := map[string]string{}
            if setItem == "" {
                continue
            }
            kvList := strings.Split(setItem, ":")

            for _, kvEle := range(kvList) {
                kv := strings.Split(kvEle, "=")
                setMap[kv[0]] = kv[1]
            }
            if _, ok := allSets[setMap["set"]]; !ok {
                allSets[setMap["set"]] = map[string]map[string]string{}
            }
            allSets[setMap["set"]][node.GetName()] = setMap
        }
    }

    return allSets
}


// Compute number of objects for given set, its total object (including replica)
func getObjectCount(setmap map[string]map[string]string, ns string) int {
    nObj := 0
    for _, m := range setmap {
        if m["ns"] != ns {
            return 0
        }
        n, _ := strconv.Atoi(m["objects"])
        nObj += n
    }
    return nObj
}


//----------------------------------------------------------------------
// Do Sync and helpers
//----------------------------------------------------------------------

// Main func to sync all records from sync.log file
func doSync() {
    // Number of threads to read from sunc.log file
    rdThread := 100
    // Channel to store record info, which failed to sync
    failedRecChan := make(chan string, 100)
    // Store rocods fetched from sync.log file
    rdChannel := make(chan string, 100)

    wg := new(sync.WaitGroup)

    // data sync will need sync log file.
    file, err := os.OpenFile(recLogFile, os.O_APPEND|os.O_RDWR, 0600)
    if err != nil {
        panicOnError(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    if err := scanner.Err(); err != nil {
        panicOnError(err)
    }

    // Parse header line
    if scanner.Scan() {
        parseHeaderLine(scanner.Text())
        // Skip next header line
        scanner.Scan()
    }

    // Track total records synced at staring of each second.
    // And upcoming next second time. will be used to throttle tps
    go func() {
        for {
            timeWEnd = time.Now().Add(time.Second)
            calcTotalRecSynced()
            recSyncedTotalOld = gStat.recSyncedTotal
            time.Sleep(time.Second)
        }
    }()

    // Read all record infoLine in channel buffer.
    go func() {
        for scanner.Scan() {
            rdChannel <- scanner.Text()
        }
        close(rdChannel)
    }()

    // Read all record from channle and sync
    for w := 1; w <= rdThread; w++ {
        wg.Add(1)
        go validateAndSync(rdChannel, failedRecChan, wg)
    }

    // Wait for all the threads to finish sync
    go func() {
        wg.Wait()
        close(failedRecChan)
    }()

    // Append failed to sync records again in sync log file
    for r := range failedRecChan {
        if _, err = file.WriteString(r); err != nil {
            panicOnError(err)
        }
    }
}


// Sync inserted or updated or deleted record. Each write will be check(gen) and write
// So if gen at destination is different from earlier recorded gen(record is
// overwritten) then it will skip but count as synced, genErr
func validateAndSync(rdChannel <-chan string, failedRecChan chan string, wg *sync.WaitGroup) {
    defer wg.Done()
    // Parse record info lines od sync.log file
    for recordLine := range rdChannel {

        // Throttle if TPS exceed limit, sleep for remaining time (1sec - time)
        calcTotalRecSynced()
        for tps > 0 && ((gStat.recSyncedTotal - recSyncedTotalOld) > tps) {
            fmt.Println("Sleeping... ")
            time.Sleep(timeWEnd.Sub(time.Now()))
        }
        syncPassed := true
        recordInfo := strings.Split(recordLine, ":")

        // Validate record log line
        if len(recordInfo) != recLineArgs {
            panicOnError(errors.New("Record log file line corrupted. param count changed infoLine: " + recordLine))
        }
        op := recordInfo[0]

        if op != deletedOp && op != insertedOp && op != updatedOp {
            panicOnError(errors.New("Record log file line corrupted, invalid op. infoLine: " + op))
        }

        // Update/Delete a record in destination if gen match with given gen
        gen64, err := strconv.ParseUint(recordInfo[3], 10, 32)
        panicOnError(err)

        gen32 := uint32(gen64)

        writePolicy := as.NewWritePolicy(0, 0)
        writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
        writePolicy.Generation = gen32

        recKey, err := getKeyFromString(recordLine)
        panicOnError(err)

        srcRec, err := srcClient.Get(readPolicy, recKey, binList...)
        //panicOnError(err)

        // In sync only case initialize stats
        if _, ok := setStats[recKey.SetName()]; !ok {
            setStats[recKey.SetName()] = &TStats{}
        }

        // Delete record, skip if its not recorded in sync.log file
        if srcRec == nil || len(srcRec.Bins) == 0 {
            syncPassed = syncDeletedRecord(recKey, op, writePolicy)

        } else if useXdr && (op == insertedOp || op == updatedOp) {
            // Touch the record xdr will send this, Assumes xdr is logging
            // records.
            // TODO: touch for specific bin and delta ship.
            // srcClient.Touch(writePolicy, recKey)

        } else {

            syncPassed = syncInsertedUpdatedRecord(srcRec, op, writePolicy)

        }

        // Failed, again append it in logfile
        // TODO: Should there be some limit in this? What if it will keep
        // failing for so long....?
        if syncPassed == false {
            failedRecChan <- getRecordLogInfoLine(op, recKey, gen32)
        }
    }
}


func syncDeletedRecord (key *as.Key, op string, writePolicy *as.WritePolicy) bool {

    stat := setStats[key.SetName()]

    if op != deletedOp {
        return true
    }

    if len(binList) == 0 {
        _, err = dstClient.Delete(writePolicy, key)

    } else {

        writePolicy.RecordExistsAction = as.UPDATE

        binMap := []*as.Bin{}

        for _, binName:= range binList {
            binMap = append(binMap, as.NewBin(binName, nil))
        }

        err = dstClient.PutBins(writePolicy, key, binMap...)
    }
    // Pass gen related error
    if err != nil && err.Error() != "Generation error" {
        panicOnError(err)
        return false

    } else {
        gStat.recSyncedDeleted++
        stat.recSyncedDeleted++
        if err != nil && err.Error() == "Generation error" {
            gStat.genErr++
            stat.genErr++
        }
    }
    return true
}


func syncInsertedUpdatedRecord(srcRec *as.Record, op string, writePolicy *as.WritePolicy) bool {

    stat := setStats[srcRec.Key.SetName()]

    failed := false
    // Insert record
    if op == insertedOp {

        writePolicy.RecordExistsAction = as.UPDATE

        err = dstClient.Put(writePolicy, srcRec.Key, srcRec.Bins)

        if err != nil && err.Error() != "Generation error" {
            panicOnError(err)
            failed = true

        } else {
            if op == insertedOp {
                gStat.recSyncedInserted++
                stat.recSyncedInserted++
            }

            if err != nil && err.Error() == "Generation error" {
                gStat.genErr++
                stat.genErr++
            }
        }
    }
    // Update record
    if op == updatedOp {

        if len(binList) == 0 {
            // Delete existing bins, fail if record doesn't exist.
            writePolicy.RecordExistsAction = as.REPLACE
            err = dstClient.Put(writePolicy, srcRec.Key, srcRec.Bins)
        } else {
            binMap := []*as.Bin{}
            for _, binName := range binList {
                if binVal, ok := srcRec.Bins[binName]; ok {
                    binMap = append(binMap, as.NewBin(binName, binVal))
                } else {
                    binMap = append(binMap, as.NewBin(binName, nil))
                }
            }
            writePolicy.RecordExistsAction = as.UPDATE
            err = dstClient.PutBins(writePolicy, srcRec.Key, binMap...)
        }

        if err != nil && err.Error() != "Generation error" {
            panicOnError(err)
            failed = true
        } else {
            if op == updatedOp {
                gStat.recSyncedUpdated++
                stat.recSyncedUpdated++
            }
            if err != nil && err.Error() == "Generation error" {
                gStat.genErr++
                stat.genErr++
            }
        }
    }

    if failed {
        return false
    }
    return true
}

//----------------------------------------------------------------------------
// Other helper functions
//----------------------------------------------------------------------------

// Parse rec info line from sync.log file and get as.Key
func getKeyFromString(keyString string) (*as.Key, error) {
    keyStruct :=  strings.Split(keyString, ":")
    keyDigest := keyStruct[1]
    byteDigest, err := hex.DecodeString(keyDigest)
    panicOnError(err)
    return as.NewKeyWithDigest(namespace, keyStruct[2], "", byteDigest)
}


// Calculate total recSynced
func calcTotalRecSynced() {
    gStat.recSyncedTotal = gStat.recSyncedUpdated + gStat.recSyncedInserted + gStat.recSyncedDeleted
}


// Main stats line printer
func printLine(setStatsMeta []string, unsyncStr string, syncStr string) {
    for _, m := range setStatsMeta {
        fmt.Printf("%30s", m)
    }

    if !syncOnly {
        fmt.Printf("%60s", unsyncStr)
    }

    if !findOnly {
        fmt.Printf("%60s", syncStr)
    }
    fmt.Println()
}


// Create set_stats to string to print
func printStat(ns string, set string, stat *TStats) {

    // Header ["Namespace", "Set", "Total_Records", "Sampled_Records",
    // "Unsync(Total, Updated, Inserted, Deleted)", "Sync(Total, Updated,
    // Inserted, Deleted, GenErr)"]

    // Print ("Namespace", "Set", "Total_Records", "Sampled_Records")
    var setStatsMeta []string
    setStatsMeta = append(setStatsMeta, ns, set, strconv.Itoa(stat.nObj), strconv.Itoa(stat.nSampleObj))

    unsyncStr := ""
    syncStr := ""

    // Print "Unsync(Total, Updated, Inserted, Deleted)"
    if !syncOnly {
        stat.recNotInSyncTotal = stat.recNotInSyncUpdated + stat.recNotInSyncInserted + stat.recNotInSyncDeleted
        unsyncStr = "(" + strconv.Itoa(stat.recNotInSyncTotal) + "," +
                        strconv.Itoa(stat.recNotInSyncUpdated) + "," +
                        strconv.Itoa(stat.recNotInSyncInserted) + "," +
                        strconv.Itoa(stat.recNotInSyncDeleted) + ")"
    }

    // Print "sync(Total, Updated, Inserted, Deleted, GenErr)"
    if !findOnly {
        stat.recSyncedTotal = stat.recSyncedUpdated + stat.recSyncedInserted + stat.recSyncedDeleted
        syncStr = "(" + strconv.Itoa(stat.recSyncedTotal) + "," +
                        strconv.Itoa(stat.recSyncedUpdated) + "," +
                        strconv.Itoa(stat.recSyncedInserted) + "," +
                        strconv.Itoa(stat.recSyncedDeleted)  + "," +
                        strconv.Itoa(stat.genErr) + ")"
    }
    printLine(setStatsMeta, unsyncStr, syncStr)

    fmt.Println(stat.reqErr)
}


// Print All set stats, global stats
func printAllStats() {
    nObj := 0
    nSampleObj := 0

    fmt.Println("\n****** Data Sync Output***")
    // Print metainfo
    fmt.Println("Modified after: " + modAfterString)
    fmt.Println("Modified before: " + modBeforeString)

    fmt.Println("\n****** set stats *********")
    // Print header
    metaList  := []string{"Namespace", "Set", "Total_Records", "Sampled_Records"}
    unsyncStr := "Unsync(Total, Updated, Inserted, Deleted)"
    syncStr   := "Sync(Total, Updated, Inserted, Deleted, GenErr)"
    printLine(metaList, unsyncStr, syncStr)

    for setname, statsObj := range setStats {
        nObj += statsObj.nObj
        nSampleObj += statsObj.nSampleObj
        printStat(namespace, setname, statsObj)
    }
    fmt.Println("\n****** Global stats ******\n")
    gStat.nObj = nObj
    gStat.nSampleObj = nSampleObj
    printStat(namespace, "", &gStat)
    fmt.Println()
}


// Get UnixNano timestamp from time
func timeStringToTimestamp(timeString string) int64 {
    if timeString == "" {
        return 0
    }
    // Get timestamp from timestring
    // TimeLayout, "Jul 5, 2017 at 11:55am (GMT)")
    parsedTime, err := time.Parse(timeLayout, timeString)
    panicOnError(err)
    return parsedTime.In(time.UTC).UnixNano()
}


// Format time to given format type
func timestampToTimeString(timestamp int64) string {
    if timestamp == 0 {
        return ""
    }
    return time.Unix(0, timestamp).Format(timeLayout)
}


// Parse and validate header line of sync file.
// TODO: validate second line too..not needed.
func parseHeaderLine(headerLine string) {
    headerOps := strings.Split(headerLine, headerDel)

    headerVer := getValidateArg(headerOps[0], "version")
    if version != headerVer {
        panicOnError(errors.New("Invalid version: " + headerVer + " expected: " + version))
    }

    modAfterString = getValidateArg(headerOps[1], "mod_after")
    modAfter = timeStringToTimestamp(modAfterString)

    modBeforeString = getValidateArg(headerOps[2], "mod_before")
    modBefore = timeStringToTimestamp(modBeforeString)

    namespace = getValidateArg(headerOps[3], "namespace")

    binString = getValidateArg(headerOps[4], "bins")
    if binString != "" {
        binList = strings.Split(binString, ",")
    }
}


// Validate header arguments name
func getValidateArg(str string, validStr string) string {
    args := strings.SplitN(str, ":", 2)
    if args[0] != validStr {
        panicOnError(errors.New("Invalid header string: " + str))
    }
    return args[1]
}


// Create a line containing op,digest,set,gen info.
func getRecordLogInfoLine(op string, key *as.Key, gen uint32) string {
    // op:digest:setname:dstRecordGen
    if op == "" {
        fmt.Println("null op")
    }
    dg := getKeyDegestString(key)
    return op + ":" + dg + ":" + key.SetName() + ":" + strconv.FormatUint(uint64(gen), 10) + "\n"
}


// Get digest string from aerospike key
func getKeyDegestString(key *as.Key) string {
    dg := key.Digest()
	hlist := make([]byte, 2*len(dg))

	for i := range dg {
		hex := fmt.Sprintf("%02x ", dg[i])
		idx := i * 2
		copy(hlist[idx:], hex)
	}
	return string(hlist)
}


func panicOnError(err error) {
	if err != nil {
        Logger.Error(err.Error())
		panic(err)
	}
}


