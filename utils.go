
/*

Package comment should be here. package description.

*/

package main

import (
	as "github.com/aerospike/aerospike-client-go"
    . "github.com/sud82/aerospike-data-sync/logger"
    "encoding/hex"
    "errors"
    "fmt"
    "os"
    "path"
    "strconv"
    "strings"
    "time"
)

type TStats struct {
    // Total checked objects
    NObj int
    // Num of Sampled objects
    NSampleObj int
    // Track total scanned records
    NScanObj int

    // Stats for records found not in sync
    RecNotInSyncTotal int
    RecNotInSyncUpdated int
    RecNotInSyncInserted int
    RecNotInSyncDeleted int

    // Stats for records synced
    RecSyncedTotal int
    RecSyncedUpdated int
    RecSyncedInserted int
    RecSyncedDeleted int

    // Generation error at (check and write) operation
    GenErr int
    // Error returned by server while scaning records
    ReqErr int
}


// Used in record sync log file. Each line will start with I,U,D based on
// operation by which that record went out of sync.
const (
    VERSION_NUM   = "1.0"

    // Type of OPeration
    INSERTED_OP   = "I"
    UPDATED_OP    = "U"
    DELETED_OP    = "D"

    FIELD_DEL  = "#"
    // HEADER keywords
    // Version:1.0#Mod_after:#Mod_before:Aug 17, 2017 at 12:14am (PDT)#Namespace:test#Bins:
    HEADER_KEYVAL_DEL = ":"

    HEADER_VERSION       = "Version"
    HEADER_MOD_AFTER     = "Mod_after"
    HEADER_MOD_BEFORE    = "Mod_before"
    HEADER_NAMESPACE     = "Namespace"
    HEADER_BINS          = "Bins"

    HEADER_OFFSET_VERSION       = 0
    HEADER_OFFSET_MOD_AFTER     = 1
    HEADER_OFFSET_MOD_BEFORE    = 2
    HEADER_OFFSET_NAMESPACE     = 3
    HEADER_OFFSET_BINS          = 4

    // 2nd header line
    HEADER_ACTION        = "Action"
    HEADER_DIGEST        = "Digest"
    HEADER_SET           = "Set"
    HEADER_GEN           = "Gen"

    // RECORD info line (OP:Digest:Set:Gen)
    REC_LINE_ARGS        = 4

    REC_LINE_OFFSET_OP   = 0
    REC_LINE_OFFSET_DG   = 1
    REC_LINE_OFFSET_SET  = 2
    REC_LINE_OFFSET_GEN  = 3
)


// Global used by all modules
var (
    // Used by time parser
    TimeLayout string      = "Jan 2, 2006 at 3:04pm (MST)"

    // AS client related
    SrcClient *as.Client = nil
    DstClient *as.Client = nil

    // Global stats to track synced, unsynced records
    GStat TStats

    // Track stats for all sets within namespace
    SetStats = map[string]*TStats{}

)

//----------------------------------------------------------------------------
// Other helper functions
//----------------------------------------------------------------------------

// Create a line containing op,digest,set,gen info.
func GetRecordLogInfoLine(op string, key *as.Key, gen uint32) string {
    // op:digest:setname:dstRecordGen
    if op == "" {
        fmt.Println("Null op")
    }
    del := FIELD_DEL
    dg := getKeyDigestString(key)
    return op + del + dg + del + key.SetName() + del + strconv.FormatUint(uint64(gen), 10) + "\n"
}


// Get digest string from aerospike key
func getKeyDigestString(key *as.Key) string {
    dg := key.Digest()
	hlist := make([]byte, 2*len(dg))

	for i := range dg {
		hex := fmt.Sprintf("%02x ", dg[i])
		idx := i * 2
		copy(hlist[idx:], hex)
	}
	return string(hlist)
}


func GetLogFileHeader(version string, modAfter int64, modBefore int64, ns string, set string, binList []string) string {
    // #ver:1#mod_after:1212334#mod_before:123233#ns:test#set:testset#bins:b1,b2,b3#

    modAfterString := timestampToTimeString(modAfter)
    modBeforeString := timestampToTimeString(modBefore)
    binString := strings.Join(binList, ",")

    hd := FIELD_DEL
    d := HEADER_KEYVAL_DEL

    return HEADER_VERSION + d + version + hd + HEADER_MOD_AFTER + d + modAfterString +
    hd + HEADER_MOD_BEFORE + d + modBeforeString + hd + HEADER_NAMESPACE + d + ns +
    hd + HEADER_BINS + d + binString + "\n" +
    HEADER_ACTION + hd + HEADER_DIGEST + hd + HEADER_SET + hd + HEADER_GEN + "\n"
}


// Format time to given format type
func timestampToTimeString(timestamp int64) string {
    if timestamp == 0 {
        return ""
    }
    return time.Unix(0, timestamp).Format(TimeLayout)
}


func InitUnsyncRecInfoFile() {
    filename := "Data" + "_" + strconv.Itoa(UnsyncRecInfoFileCount)
    filepath := path.Join(UnsyncRecInfoDir, filename)
    UnsyncRecInfoFileCount++
    UnsyncRecInfoFile = filepath
    // create and write header in file
    Logger.Info("Create new Unsync Record info file: %s", filepath)
    file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0600)
    if err != nil {
        PanicOnError(err)
    }
    defer file.Close()

    header := GetLogFileHeader(VERSION_NUM, ModAfter, ModBefore, Namespace, Set, BinList)
    if _, err = file.WriteString(header); err != nil {
        PanicOnError(err)
    }
}


func PanicOnError(err error) {
	if err != nil {
        Logger.Error(err.Error())
		panic(err)
	}
}

//---------------------------------------------------------------------------
// Do Sync
//---------------------------------------------------------------------------
// Parse and validate header line of unsync_record_info file.
// TODO: validate second line too..not needed.
func ParseHeaderLine(headerLine string) {
    headerOps := strings.Split(headerLine, FIELD_DEL)

    headerVer := getValidateArg(headerOps[HEADER_OFFSET_VERSION], HEADER_VERSION)
    if VERSION_NUM != headerVer {
        PanicOnError(errors.New("Invalid version: " + headerVer + " expected: " + VERSION_NUM))
    }

    ModAfterString = getValidateArg(headerOps[HEADER_OFFSET_MOD_AFTER], HEADER_MOD_AFTER)
    ModAfter = timeStringToTimestamp(ModAfterString)

    ModBeforeString = getValidateArg(headerOps[HEADER_OFFSET_MOD_BEFORE], HEADER_MOD_BEFORE)
    ModBefore = timeStringToTimestamp(ModBeforeString)

    Namespace = getValidateArg(headerOps[HEADER_OFFSET_NAMESPACE], HEADER_NAMESPACE)

    BinString = getValidateArg(headerOps[HEADER_OFFSET_BINS], HEADER_BINS)
    if BinString != "" {
        BinList = strings.Split(BinString, ",")
    }
}


// Validate header arguments name
func getValidateArg(str string, validStr string) string {
    args := strings.SplitN(str, HEADER_KEYVAL_DEL, 2)
    if args[0] != validStr {
        PanicOnError(errors.New("Invalid header string: " + str))
    }
    return args[1]
}


// Get UnixNano timestamp from time
func timeStringToTimestamp(timeString string) int64 {
    if timeString == "" {
        return 0
    }
    // Get timestamp from timestring
    // TimeLayout, "Jul 5, 2017 at 11:55am (GMT)")
    parsedTime, err := time.Parse(TimeLayout, timeString)
    PanicOnError(err)
    return parsedTime.In(time.UTC).UnixNano()
}

// Parse rec info line from unsync_record_info file and get as.Key
func GetKeyFromString(ns string, recInfoLine string) (*as.Key, error) {
    recInfoList :=  strings.Split(recInfoLine, FIELD_DEL)
    keyDigest := recInfoList[REC_LINE_OFFSET_DG]
    byteDigest, err := hex.DecodeString(keyDigest)
    PanicOnError(err)
    return as.NewKeyWithDigest(ns, recInfoList[REC_LINE_OFFSET_SET], "", byteDigest)
}


// Calculate total recSynced
func CalcTotalRecSynced(stat *TStats) {
    stat.RecSyncedTotal = stat.RecSyncedUpdated + stat.RecSyncedInserted + stat.RecSyncedDeleted
}

