/*

Package comment should be here. package description.

*/

package main

import (
	as "github.com/aerospike/aerospike-client-go"
    //. "github.com/sud82/aerospike-data-sync/logger"
    "bufio"
    "errors"
    "fmt"
    "os"
    "path"
    "io/ioutil"
    "strings"
    "sync"
    "strconv"
    "time"
)

// For threshold, Time window (track time after 1 sec)
var (
    timeWEnd = time.Now()
    recSyncedTotalOld int = 0
)

//----------------------------------------------------------------------
// Do Sync and helpers
//----------------------------------------------------------------------

var Count int = 0

func DoSync() {
    //err := filepath.Walk(UnsyncRecInfoDir, doSyncForFile)
    files,_ := ioutil.ReadDir(UnsyncRecInfoDir)
    //UnsyncRecInfoFileCount = len(files)

    for _, filepath := range files {
        doSyncForFile(path.Join(UnsyncRecInfoDir,filepath.Name()))
    }
}

// Main func to sync all records from all unsyncRecord info files
func doSyncForFile(filepath string) {
    fmt.Println("func")
    // Number of threads to read from unsync_record_info file
    rdThread := 100
    // Channel to store record info, which failed to sync
    failedRecChan := make(chan string, 100)
    // Store rocods fetched from unsyncRecordInfo file
    rdChannel := make(chan string, 100)

    wg := new(sync.WaitGroup)

    // data sync will need unsync_record_info file.
    file, err := os.OpenFile(filepath, os.O_APPEND|os.O_RDWR, 0600)
    if err != nil {
        PanicOnError(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    if err := scanner.Err(); err != nil {
        PanicOnError(err)
    }

    // Parse header line
    if scanner.Scan() {
        ParseHeaderLine(scanner.Text())
        // Skip next header line
        scanner.Scan()
    }

    // Track total records synced at staring of each second.
    // And upcoming next second time. will be used to throttle tps
    go func() {
        for {
            timeWEnd = time.Now().Add(time.Second)
            CalcTotalRecSynced(&GStat)
            recSyncedTotalOld = GStat.RecSyncedTotal
            time.Sleep(time.Second)
        }
    }()

    // Read all record infoLine in channel buffer.
    go func() {
        for scanner.Scan() {
            rdChannel <- scanner.Text()
            Count++
            //fmt.Println(Count)
        }
        close(rdChannel)
    }()

    // Read all record from channle and sync
    for w := 1; w <= rdThread; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            validateAndSync(SrcClient, DstClient, rdChannel, failedRecChan, Tps)
        }()
    }

    // Wait for all the threads to finish sync
    go func() {
        wg.Wait()
        close(failedRecChan)
    }()

    // Append failed to sync records again in unsync_record_info file
    // TODO: There should be some limit for number of errors..
    for r := range failedRecChan {
        if _, err = file.WriteString(r); err != nil {
            PanicOnError(err)
        }
    }

}


// Sync inserted or updated or deleted record. Each write will be check(gen) and write
// So if gen at destination is different from earlier recorded gen(record is
// overwritten) then it will skip but count as synced, GenErr
func validateAndSync(srcClient *as.Client, dstClient *as.Client, rdChannel <-chan string, failedRecChan chan string, tps int) {
    // Parse record info lines from unsync_record_info file
    for recInfoLine := range rdChannel {

        // Throttle if TPS exceed limit, sleep for remaining time (1sec - time)
        CalcTotalRecSynced(&GStat)
        for tps > 0 && ((GStat.RecSyncedTotal - recSyncedTotalOld) > tps) {
            fmt.Println("Sleeping... ")
            time.Sleep(timeWEnd.Sub(time.Now()))
        }
        syncPassed := true
        recInfoList := strings.Split(recInfoLine, FIELD_DEL)

        // Validate record log line REC_LINE_ARGS=4(OP#Digest#Set#Gen)
        if len(recInfoList) != REC_LINE_ARGS {
            PanicOnError(errors.New("Record log file line corrupted. param count changed infoLine: " + recInfoLine))
        }
        // TODO: check num
        op := recInfoList[REC_LINE_OFFSET_OP]

        if op != DELETED_OP && op != INSERTED_OP && op != UPDATED_OP {
            PanicOnError(errors.New("Record log file line corrupted, invalid op. infoLine: " + op))
        }

        //TODO: Is there a way to directly convert gen to uint32
        // Update/Delete a record in destination if gen match with given gen
        gen64, err := strconv.ParseUint(recInfoList[REC_LINE_OFFSET_GEN], 10, 32)
        PanicOnError(err)

        gen32 := uint32(gen64)

        writePolicy := as.NewWritePolicy(0, 0)
        writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
        writePolicy.Generation = gen32

        recKey, err := GetKeyFromString(Namespace, recInfoLine)
        PanicOnError(err)

        readPolicy := as.NewPolicy()
        srcRec, err := srcClient.Get(readPolicy, recKey, BinList...)
        //PanicOnError(err)

        // In sync only case initialize stats
        if _, ok := SetStats[recKey.SetName()]; !ok {
            SetStats[recKey.SetName()] = &TStats{}
        }

        // Delete record, skip if its not recorded in unsync_record_info file
        if srcRec == nil {
            syncPassed = syncDeletedRecord(dstClient, recKey, op, writePolicy)

        } else if UseXdr && (op == INSERTED_OP || op == UPDATED_OP) {
            // Touch the record xdr will send this, Assumes xdr is logging
            // records.
            // TODO: touch for specific bin and delta ship.
            // srcClient.Touch(writePolicy, recKey)

        } else {

            syncPassed = syncInsertedUpdatedRecord(dstClient, srcRec, op, writePolicy)

        }

        // Failed, again append it in logfile
        // TODO: Should there be some limit in this? What if it will keep
        // failing for so long....?
        if syncPassed == false {
            failedRecChan <- GetRecordLogInfoLine(op, recKey, gen32)
        }
    }
}


func syncDeletedRecordOld(dstClient *as.Client, key *as.Key, op string, writePolicy *as.WritePolicy) bool {

    stat := SetStats[key.SetName()]

    if op != DELETED_OP {
        return true
    }

    // If Binlist == 0, then full record must be deleted
    if len(BinList) == 0 {
        _, err = dstClient.Delete(writePolicy, key)

    } else {

        writePolicy.RecordExistsAction = as.UPDATE

        binMap := []*as.Bin{}

        for _, binName:= range BinList {
            binMap = append(binMap, as.NewBin(binName, nil))
        }

        // TODO: check all conditions when reading writing
        err = dstClient.PutBins(writePolicy, key, binMap...)
    }
    // Pass gen related error
    if err != nil && err.Error() != "Generation error" {
        PanicOnError(err)
        return false

    } else {
        GStat.RecSyncedDeleted++
        stat.RecSyncedDeleted++
        if err != nil && err.Error() == "Generation error" {
            GStat.GenErr++
            stat.GenErr++
        }
    }
    return true
}

func syncDeletedRecord(dstClient *as.Client, key *as.Key, op string, writePolicy *as.WritePolicy) bool {

    stat := SetStats[key.SetName()]

    if op != DELETED_OP {
        return true
    }

    writePolicy.RecordExistsAction = as.UPDATE

    _, err = dstClient.Delete(writePolicy, key)

    // Pass gen related error
    if err != nil && err.Error() != "Generation error" {
        PanicOnError(err)
        return false

    } else {
        GStat.RecSyncedDeleted++
        stat.RecSyncedDeleted++
        if err != nil && err.Error() == "Generation error" {
            GStat.GenErr++
            stat.GenErr++
        }
    }
    return true
}



func syncInsertedUpdatedRecord(dstClient *as.Client, srcRec *as.Record, op string, writePolicy *as.WritePolicy) bool {

    stat := SetStats[srcRec.Key.SetName()]

    failed := false
    // Insert record
    if op == INSERTED_OP {

        writePolicy.RecordExistsAction = as.UPDATE

        err = dstClient.Put(writePolicy, srcRec.Key, srcRec.Bins)

        if err != nil && err.Error() != "Generation error" {
            PanicOnError(err)
            failed = true

        } else {
            if op == INSERTED_OP {
                GStat.RecSyncedInserted++
                stat.RecSyncedInserted++
            }

            if err != nil && err.Error() == "Generation error" {
                GStat.GenErr++
                stat.GenErr++
            }
        }
    }
    // Update record
    if op == UPDATED_OP {

        // If binlist == 0 then full record must be replaced.
        if len(BinList) == 0 {
            // Delete existing bins, fail if record doesn't exist.
            writePolicy.RecordExistsAction = as.REPLACE
            err = dstClient.Put(writePolicy, srcRec.Key, srcRec.Bins)
        } else {
            binMap := []*as.Bin{}
            // Bins which are not present in src_rec would be deleted. others
            // updated
            for _, binName := range BinList {
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
            PanicOnError(err)
            failed = true
        } else {
            if op == UPDATED_OP {
                GStat.RecSyncedUpdated++
                stat.RecSyncedUpdated++
            }
            if err != nil && err.Error() == "Generation error" {
                GStat.GenErr++
                stat.GenErr++
            }
        }
    }

    if failed {
        return false
    }
    return true
}

