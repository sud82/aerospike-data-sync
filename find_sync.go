/*

Package comment should be here. package description.

*/

package main

import (
	as "github.com/aerospike/aerospike-client-go"
    . "github.com/sud82/aerospike-data-sync/logger"
    "errors"
    "fmt"
    "os"
    "reflect"
    "strings"
    "sync"
    "strconv"
    "time"
)


//----------------------------------------------------------------------
// Find not in sync func and helpers
//----------------------------------------------------------------------
// Main func to found records not in sync
func FindRecordsNotInSync() {
    Logger.Info("Find records not in sync")
    wg := new(sync.WaitGroup)


    // Channel to store unsync record's info, max 100000*50 = 5MB record info at a time
    recordInfoChan := make(chan string, 100000)

    // Open unsync_record_info log file to write found unsync records
    var file *os.File = nil
    if UnsyncRecInfoFile != "" {
        file, err = os.OpenFile(UnsyncRecInfoFile, os.O_APPEND|os.O_WRONLY, 0600)
        if err != nil {
            PanicOnError(err)
        }
        defer file.Close()
    }

    // Add progress indicator
    go func() {
        fmt.Printf("Progress")
        for {
            fmt.Printf(".")
            time.Sleep(100*time.Millisecond)
        }
    }()

    // Get replication factor
    replFact := getReplicationFact(SrcClient, Namespace)
    if replFact == 0 {
        PanicOnError(errors.New("Coundn't get replication factor for NS: " + Namespace + ". check Config."))
    }

    // Parsed sets Stats fetched from source aesospike server
    allSetStatsMap := getSetMap(SrcClient, Namespace)

    // Scan records from source and validate them by running
    // multiple validation threads 
    for setname, statsMap := range allSetStatsMap {

        if Set != "" && Set != setname {
            continue
        }

        // It gives all objects so divide by repl factor
        nObj := getObjectCount(statsMap, Namespace)
        nObj = nObj / replFact
        if nObj == 0 {
            continue
        }

        // Update set stats
        SetStats[setname] = &TStats{}
        SetStats[setname].NObj = nObj

        sz := 0
        if SamplePer != 0 {
            sz = nObj * SamplePer / 100
            Logger.Info("Sample percentage given, Sample Size: " + strconv.Itoa(sz))
        } else {
            sz = SampleSz
            Logger.Info("Sample percentage not given, Sample Size: " + strconv.Itoa(sz))
        }

        SetStats[setname].NSampleObj = sz

        srcRecordset := getRecordset(SrcClient, Namespace, setname, BinList, ModAfter, ModBefore)

        // Run multiple thread to fetch records from queryRecordQueue
        // and validate those records to see if they are in sync or not
        for w := 0; w < FindSyncThread; w++ {
            wg.Add(1)

            go func(setname string) {
                defer wg.Done()

                validateAndFindInsertedUpdated(srcRecordset,  setname, BinList, DstClient, recordInfoChan)
            }(setname)
        }
    }

    // Wait for completing all threads and closing recordInfoChan
    go func() {
        wg.Wait()
        close(recordInfoChan)
    }()

    // Continue looping if record info log file doesn't exist.
    // This has to wait for closing recordInfoChan
    fileLineCount := 0
    for r := range recordInfoChan {
        if UnsyncRecInfoFile == "" {
            continue
        }

        // 100000 ~= 5MB file
        if fileLineCount == 100000 {
            file.Close()
            // Init new file
            InitUnsyncRecInfoFile()

            file, err = os.OpenFile(UnsyncRecInfoFile, os.O_APPEND|os.O_WRONLY, 0600)
            if err != nil {
                PanicOnError(err)
            }
            fileLineCount = 0
        }

        if _, err = file.WriteString(r); err != nil {
            PanicOnError(err)
        }
        fileLineCount++
    }

    defer file.Close()
}


// Validate and find records not in Sync
// Inserted, Updated: Records which are not replicated to destination
// Deleted: Records which are deleted in source but not in destination
// Note: Its not possible to find deleted unsynced records in A-A topology
func validateAndFindInsertedUpdated(srcRecordset *as.Recordset, setname string, binList []string, dstClient *as.Client, recordInfoChan chan string) {
    Logger.Info("Thread to fetch and match src and dst records. SET: %s", setname)
    Logger.Info("Find Updated, Inserted record if not in sync.")
    sStat := SetStats[setname]
L1:
	for {

		select {

		case srcRec := <-srcRecordset.Records:
            // Break If scan bucket is giving more then sampled object
			if srcRec == nil || (sStat.NScanObj >= sStat.NSampleObj) {
                Logger.Info("Src, Sample limit reached or No record left to match. SET: %s", setname)
                Logger.Info("Src, Scanned records: %s, Sample Size: %s. SET: %s",
                    strconv.Itoa(sStat.NScanObj),
                    strconv.Itoa(sStat.NSampleObj), setname)
                break L1
			}
            sStat.NScanObj++

            // TODO: Add LUT check for record. LUT. skip if srcRecord.LUT > Timestamp
            dstRec, err := dstClient.Get(ReadPolicy, srcRec.Key, binList...)
            if err != nil {
                sStat.FindSync.Err++
                Logger.Debug("Find Inserted/Updated Record_Get From Destination cluster. error: " + err.Error())
            }

            // If rec doesn't exist in dst, it's new insert in src. log it. Add gen = 0 for new rec
            if dstRec == nil {
                var gen uint32 = 0

                recordInfoChan <- GetRecordLogInfoLine(INSERTED_OP, srcRec.Key, gen)
                sStat.RecNotInSyncInserted++

                Logger.Debug("Record op Insert. setStat_RecNotInSync: %s. SET: %s",
                    strconv.Itoa(sStat.RecNotInSyncInserted), setname)
                continue
            }

            // src and dst record doesn't match. Record Updated. log it.
            if !reflect.DeepEqual(srcRec.Bins, dstRec.Bins) {
                //fmt.Println("src")
                //fmt.Println("dst")
                //fmt.Println()

                recordInfoChan <- GetRecordLogInfoLine(UPDATED_OP, srcRec.Key, dstRec.Generation)
                sStat.RecNotInSyncUpdated++
                Logger.Debug("Record op Update. setStat_RecNotInSync: %s. SET: %s",
                    strconv.Itoa(sStat.RecNotInSyncUpdated), setname)
                continue

            }

		case err := <-srcRecordset.Errors:
            if err != nil {
                Logger.Debug("Record read error: %s. SET: %s", err.Error(), setname)
                sStat.FindSync.ScanReqErr++
            }
			//fmt.Println(err)
            continue
		}
	}

}


// Validate and find records not in Sync
// Inserted, Updated: Records which are not replicated to destination
// Deleted: Records which are deleted in source but not in destination
// Note: Its not possible to find deleted unsynced records in A-A topology
func validateAndFindDeleted(dstRecordset *as.Recordset, setname string, binList []string, srcClient *as.Client, recordInfoChan chan string,) {
    Logger.Info("Thread to fetch and match src and dst records. SET: %s", setname)
    Logger.Info("Find Updated, Inserted record if not in sync.")
    sStat := SetStats[setname]
L2:
	for {
		select {
		case dstRec := <-dstRecordset.Records:
	        // Break If scan bucket is giving more then sampled object
            if (dstRec == nil) {
                Logger.Info("Dst: No record left to match. SET: %s", setname)
				break L2
			}

            srcRec, err := srcClient.Get(ReadPolicy, dstRec.Key, binList...)
            if err != nil {
                sStat.FindSync.Err++
                Logger.Error("Find Deleted. Record_Get From Source cluster. error: " + err.Error())
            }

            if  srcRec == nil {
                recordInfoChan <- GetRecordLogInfoLine(DELETED_OP, dstRec.Key, dstRec.Generation)
                sStat.RecNotInSyncDeleted++
                Logger.Debug("Record op Delete. setStat_RecNotInSync: %s. SET: %s",
                    strconv.Itoa(sStat.RecNotInSyncDeleted), setname)

                continue
            }

            // src and dst record doesn't match. Record Updated. log it.
            if !reflect.DeepEqual(srcRec.Bins, dstRec.Bins) {
                //fmt.Println("src")
                //fmt.Println("dst")
                //fmt.Println()

                recordInfoChan <- GetRecordLogInfoLine(UPDATED_OP, srcRec.Key, dstRec.Generation)
                sStat.RecNotInSyncUpdated++
                Logger.Debug("Record op Update. setStat_RecNotInSync: %s. SET: %s",
                    strconv.Itoa(sStat.RecNotInSyncUpdated), setname)
                continue

            }

		case err := <-dstRecordset.Errors:
            if err != nil {
                Logger.Debug("Record read error: %s. SET: %s", err.Error(), setname)
                sStat.FindSync.ScanReqErr++
            }
            continue
		}
	}
}


// Scan all records in given timerange
func getRecordset(client *as.Client, ns string, set string,  binList []string, modAfter int64, modBefore int64) *as.Recordset {
    Logger.Info("Send query and create RecordSet. NS: %s, SET: %s, BINLIST: %s", ns, set, binList)
    stm := as.NewStatement(ns, set, binList...)

    createTimeRangeStm(stm, modAfter, modBefore)

    recordset, err := client.Query(QueryPolicy, stm)

    PanicOnError(err)
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
        PanicOnError(err)
        if replFact, ok := info["repl-factor"]; ok {
            r, err := strconv.Atoi(replFact)
            PanicOnError(err)
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
        PanicOnError(err)
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

