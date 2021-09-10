package main

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	timestamp "github.com/golang/protobuf/ptypes/timestamp"

	pb "github.com/synerex/synerex_api"
	sxutil "github.com/synerex/synerex_sxutil"
)

// datastore provider provides Datastore Service.

type DataStore interface {
	store(str string)
}

var (
	nodesrv     = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	channel     = flag.String("channel", "3", "Retrieving channel type(default 3, support comma separated)")
	local       = flag.String("local", "", "Specify Local Synerex Server")
	startDate   = flag.String("startDate", "01-01", "Specify Start Date")
	endDate     = flag.String("endDate", "12-31", "Specify End Date")
	startTime   = flag.String("startTime", "00:00", "Specify Start Time")
	endTime     = flag.String("endTime", "24:00", "Specify End Time")
	dir         = flag.String("dir", "xbanddata", "Directory of data storage") // for all file
	all         = flag.Bool("all", true, "Send all file in data storage")      // for all file
	completion  = flag.Bool("completion", false, "Completing data between xband data")
	miniGapTime = flag.Int("miniGapTime", 60, "Minimum time difference. (seconds)") // for completion true
	compdiv     = flag.Int("compdiv", 6, "Number of splits.")                       // for completion true
	verbose     = flag.Bool("verbose", false, "Verbose information")
	jst         = flag.Bool("jst", false, "Run/display with JST Time")
	recTime     = flag.Bool("recTime", false, "Send with recorded time")
	speed       = flag.Float64("speed", 1.0, "Speed of sending packets(default real time =1.0), minus in msec")
	skip        = flag.Int("skip", 0, "Skip lines(default 0)")
)

var sendfile string

const dateFmt = "2006-01-02T15:04:05.999Z"

func atoUint(s string) uint32 {
	r, err := strconv.Atoi(s)
	if err != nil {
		log.Print("err", err)
	}
	return uint32(r)
}

func getHourMin(dt string) (hour int, min int) {
	st := strings.Split(dt, ":")
	hour, _ = strconv.Atoi(st[0])
	min, _ = strconv.Atoi(st[1])
	return hour, min
}

func getMonthDate(dt string) (month int, date int) {
	st := strings.Split(dt, "-")
	month, _ = strconv.Atoi(st[0])
	date, _ = strconv.Atoi(st[1])
	return month, date
}

func NotifySupplyWithTime(clt *sxutil.SXServiceClient, smo *sxutil.SupplyOpts, ts *timestamp.Timestamp) (uint64, error) {
	id := sxutil.GenerateIntID()
	dm := pb.Supply{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: clt.ChannelType,
		SupplyName:  smo.Name,
		Ts:          ts,
		ArgJson:     smo.JSON,
		Cdata:       smo.Cdata,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//	resp , err := clt.Client.NotifySupply(ctx, &dm)

	_, err := clt.SXClient.Client.NotifySupply(ctx, &dm)
	if err != nil {
		log.Printf("Error for sending:NotifySupply to Synerex Server as %v ", err)
		return 0, err
	}
	//	log.Println("RegiterSupply:", smo, resp)
	smo.ID = id // assign ID
	return id, nil
}

type DataHeader struct {
	Header1    [8]byte
	DateTime   [16]byte
	SystemSts  [16]byte
	Header2    [2]byte
	BlockCount uint16
	DataSize   uint32
	DataID4    [2]byte
	DataID5    [2]byte
	Reserve    [12]byte
}
type BlockHeader struct {
	Base_lat uint8
	Base_lon uint8
	Mash2    byte
	Cell_max uint8
}
type BlockData struct {
	Cell [40][40]uint16
}
type Gridcelldata struct {
	Position  [2]float64 `json:"position"`
	Color     [3]int     `json:"color"`
	Elevation float64    `json:"elevation"`
}
type Operation struct {
	Elapsedtime  int           `json:"elapsedtime"`
	Gridcelldata []interface{} `json:"gridcelldata"`
}
type Movesbase struct {
	MeshId    string      `json:"meshId"`
	Operation []Operation `json:"operation"`
}

const unit_lat = 2.0 / (3.0 * 8.0 * 40.0) // 緯度２度あたり960個のセル
const unit_lon = 1.0 / (8.0 * 40.0)       // 経度１度あたり320個のセル？

func pallet(rainfall float64) [3]int {
	var rate float64
	var sourceColor [3]int
	var targetColor [3]int
	if rainfall > 150 {
		rate = (math.Min(rainfall-150, 50) * 10) / 500
		sourceColor = [3]int{180, 0, 104}
		targetColor = [3]int{64, 0, 0}
	} else if rainfall > 100 {
		rate = ((rainfall - 100) * 10) / 500
		sourceColor = [3]int{255, 40, 0}
		targetColor = [3]int{180, 0, 104}
	} else if rainfall > 50 {
		rate = ((rainfall - 50) * 10) / 500
		sourceColor = [3]int{255, 153, 0}
		targetColor = [3]int{255, 40, 0}
	} else if rainfall > 30 {
		rate = ((rainfall - 30) * 10) / 200
		sourceColor = [3]int{250, 245, 0}
		targetColor = [3]int{255, 153, 0}
	} else if rainfall > 20 {
		rate = ((rainfall - 20) * 10) / 100
		sourceColor = [3]int{0, 65, 255}
		targetColor = [3]int{250, 245, 0}
	} else if rainfall > 10 {
		rate = ((rainfall - 10) * 10) / 100
		sourceColor = [3]int{33, 140, 255}
		targetColor = [3]int{0, 65, 255}
	} else if rainfall > 0 {
		rate = (rainfall * 10) / 100
		sourceColor = [3]int{255, 255, 255}
		targetColor = [3]int{33, 140, 255}
	} else {
		rate = 1.0
		sourceColor = [3]int{255, 255, 255}
		targetColor = [3]int{255, 255, 255}
	}
	return [3]int{
		int(float64(sourceColor[0]) + (rate * float64(targetColor[0]-sourceColor[0]))),
		int(float64(sourceColor[1]) + (rate * float64(targetColor[1]-sourceColor[1]))),
		int(float64(sourceColor[2]) + (rate * float64(targetColor[2]-sourceColor[2])))}
}

func conversionXbandJson(dfile string) (string, Operation, bool) {

	var dataheader DataHeader
	var blockHeader BlockHeader
	var blockData BlockData

	log.Printf("fileName %s", dfile)

	fp, err := os.Open(dfile)
	if err != nil {
		return "", Operation{}, false
	}
	defer fp.Close()
	gr, err := gzip.NewReader(fp)
	if err != nil {
		return "", Operation{}, false
	}
	defer gr.Close()

	binary.Read(gr, binary.BigEndian, &dataheader)
	log.Printf("DataHeader %x", dataheader)
	var yaer, month, date, hour, minute int
	fmt.Sscanf(fmt.Sprintf("%s", dataheader.DateTime), "%4d.%2d.%2d.%2d.%2d", &yaer, &month, &date, &hour, &minute)
	elapsedtime := int(time.Date(yaer, time.Month(month), date, hour, minute, 0, 0, time.Local).Unix())
	log.Printf("elapsedtime %d", elapsedtime)
	gridcelldata := make([]interface{}, 0)
	firstmeshID := fmt.Sprintf("%04x", dataheader.DataID4)
	log.Printf("firstmeshID %s", firstmeshID)
	emptyData := make(map[string]interface{})

	for i := 0; i < int(dataheader.BlockCount); i++ {
		binary.Read(gr, binary.BigEndian, &blockHeader)
		base_lat := float64(blockHeader.Base_lat) / 1.5
		base_lon := float64(blockHeader.Base_lon) + 100
		mash2_lat := blockHeader.Mash2 >> 4
		mash2_lon := blockHeader.Mash2 & 0b1111
		//log.Printf("%f %f %d %d %d", base_lat, base_lon, mash2_lat, mash2_lon, blockHeader.Cell_max)
		for j := 0; j < int(blockHeader.Cell_max); j++ {
			binary.Read(gr, binary.BigEndian, &blockData)
			for k := 0; k < len(blockData.Cell); k++ {
				for l := 0; l < len(blockData.Cell[k]); l++ {
					if blockData.Cell[k][l]&0b1000000000000000 != 0 {
						cell_lat := base_lat + ((((float64(mash2_lat) + 1.0) * 40.0) - float64(k)) * unit_lat)        //4分の1 3次メッシュ 北端
						cell_lon := base_lon + ((((float64(mash2_lon) + float64(j)) * 40.0) + float64(l)) * unit_lon) //4分の1 3次メッシュ 西端
						rainfall := float64(blockData.Cell[k][l]&0b0000111111111111) / 10
						if rainfall > 0 {
							gridcelldata = append(gridcelldata, Gridcelldata{Position: [2]float64{cell_lon, cell_lat},
								Color: pallet(rainfall), Elevation: rainfall})
							//log.Printf("%f %f %f", cell_lat, cell_lon, rainfall)
						} else {
							gridcelldata = append(gridcelldata, emptyData)
						}
					}
				}
			}
		}
	}
	return firstmeshID, Operation{Elapsedtime: elapsedtime, Gridcelldata: gridcelldata}, true
}

func conversionAllXbandJson() ([]Movesbase, bool) {
	now := time.Now()

	stMonth, stDate := getMonthDate(*startDate)
	stHour, stMin := getHourMin(*startTime)
	stDateUnix := time.Date(now.Year(), time.Month(stMonth), stDate, stHour, stMin, 0, 0, time.Local).Unix()
	log.Printf("stDateUnix %d", stDateUnix)

	edMonth, edDate := getMonthDate(*endDate)
	edHour, edMin := getHourMin(*endTime)
	edDateUnix := time.Date(now.Year(), time.Month(edMonth), edDate, edHour, edMin, 0, 0, time.Local).Unix()
	log.Printf("edDateUnix %d", edDateUnix)

	if *dir == "" {
		log.Printf("Please specify directory")
		data := "data"
		dir = &data
	}
	files, err := ioutil.ReadDir(*dir)
	if err != nil {
		log.Printf("Can't open diretory %v", err)
		os.Exit(1)
	}

	// should be sorted.
	fileNames := make(sort.StringSlice, 0, len(files))

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".gz") { // check is CSV file
			//
			fn := file.Name()
			var id, g, el string
			var yaer, month, date, hour, minute int
			ct, _ := fmt.Sscanf(fn, "%10s-%4d%2d%2d-%2d%2d-G%3s-EL%6s.gz", &id, &yaer, &month, &date, &hour, &minute, &g, &el)
			fileDateUnix := time.Date(yaer, time.Month(month), date, hour, minute, 0, 0, time.Local).Unix()

			if ct > 0 && stDateUnix <= fileDateUnix && fileDateUnix <= edDateUnix {
				fileNames = append(fileNames, file.Name())
			} else {
				log.Printf("eject file: %d %s", ct, fn)
			}
		}
	}

	fileNames.Sort()

	meshDatabase := map[string][]Operation{}

	for _, fileName := range fileNames {

		dfile := path.Join(*dir, fileName)

		firstmeshID, addOperation, result := conversionXbandJson(dfile)
		if !result {
			return []Movesbase{}, result
		}
		operation, ok := meshDatabase[firstmeshID]
		if !ok {
			operation = make([]Operation, 0)
		}
		if *completion {
			operation = elapsedDataGeneration(operation, addOperation)
		}
		operation = append(operation, addOperation)
		meshDatabase[firstmeshID] = operation
	}
	movesbase := make([]Movesbase, 0)
	for meshId, operation := range meshDatabase {
		movesbase = append(movesbase, Movesbase{MeshId: meshId, Operation: operation})
	}
	return movesbase, true
}

func elapsedDataGeneration(operation []Operation, addOperation Operation) []Operation {
	operationLength := len(operation)
	minimumGapTime := *miniGapTime
	divisions := *compdiv
	if operationLength > 0 {
		beforeElapsedtime := operation[operationLength-1].Elapsedtime
		afterElapsedtime := addOperation.Elapsedtime
		if (afterElapsedtime - beforeElapsedtime) >= minimumGapTime {
			beforeGridcelldata := operation[operationLength-1].Gridcelldata
			afterGridcelldata := addOperation.Gridcelldata
			if len(beforeGridcelldata) == len(afterGridcelldata) {
				gapTime := int((afterElapsedtime - beforeElapsedtime) / divisions)
				generateNum := int((afterElapsedtime - beforeElapsedtime) / gapTime)
				for i := 0; i < int(generateNum); i++ {
					gridcelldata := make([]interface{}, 0)
					for dataidx, beforedata := range beforeGridcelldata {
						afterdata := afterGridcelldata[dataidx]
						if fmt.Sprintf("%v", reflect.TypeOf(beforedata)) == "main.Gridcelldata" &&
							fmt.Sprintf("%v", reflect.TypeOf(afterdata)) == "main.Gridcelldata" {
							rainfall := beforedata.(Gridcelldata).Elevation
							afterRainfall := afterdata.(Gridcelldata).Elevation
							if rainfall != afterRainfall {
								values := (math.Abs(rainfall-afterRainfall) * (float64(i+1) / float64(generateNum)))
								if rainfall > afterRainfall {
									rainfall = rainfall - values
								} else {
									rainfall = rainfall + values
								}
							}
							if rainfall >= 0.1 {
								gridcelldata = append(gridcelldata, Gridcelldata{Position: beforedata.(Gridcelldata).Position,
									Color: pallet(rainfall), Elevation: rainfall})
							}
						} else if fmt.Sprintf("%v", reflect.TypeOf(beforedata)) != "main.Gridcelldata" &&
							fmt.Sprintf("%v", reflect.TypeOf(afterdata)) == "main.Gridcelldata" {
							rainfall := float64(0)
							afterRainfall := afterdata.(Gridcelldata).Elevation
							if rainfall != afterRainfall {
								values := (math.Abs(rainfall-afterRainfall) * (float64(i+1) / float64(generateNum)))
								rainfall = rainfall + values
							}
							if rainfall >= 0.1 {
								gridcelldata = append(gridcelldata, Gridcelldata{Position: afterdata.(Gridcelldata).Position,
									Color: pallet(rainfall), Elevation: rainfall})
							}
						} else if fmt.Sprintf("%v", reflect.TypeOf(beforedata)) == "main.Gridcelldata" &&
							fmt.Sprintf("%v", reflect.TypeOf(afterdata)) != "main.Gridcelldata" {
							rainfall := beforedata.(Gridcelldata).Elevation
							afterRainfall := float64(0)
							if rainfall != afterRainfall {
								values := (math.Abs(rainfall-afterRainfall) * (float64(i+1) / float64(generateNum)))
								rainfall = rainfall - values
							}
							if rainfall >= 0.1 {
								gridcelldata = append(gridcelldata, Gridcelldata{Position: beforedata.(Gridcelldata).Position,
									Color: pallet(rainfall), Elevation: rainfall})
							}
						}
					}
					operation = append(operation, Operation{Elapsedtime: beforeElapsedtime + (gapTime * (i + 1)), Gridcelldata: gridcelldata})
				}
			}
		}
		return operation
	} else {
		return operation
	}
}

func sendAllStoredFile(clients map[uint32]*sxutil.SXServiceClient) {
	// check all files in dir.
	movesbase, result := conversionAllXbandJson()
	if !result {
		os.Exit(1)
	}
	//jsonFile := map[string][]Movesbase{"movesbase":movesbase}
	//jsonFile, _ := json.Marshal(movesbase)

	//log.Printf("movesbase %s", string(jsonFile))

	dfile := path.Join(*dir, "output.json")
	f, err := os.Create(dfile)
	if err != nil {
		os.Exit(1)
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(movesbase)
	if err != nil {
		os.Exit(1)
	}
}

//dataServer(pc_client)

func main() {
	log.Printf("ChannelXband(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	// check channel types.
	//
	channelTypes := []uint32{}
	chans := strings.Split(*channel, ",")
	for _, ch := range chans {
		v, err := strconv.Atoi(ch)
		if err == nil {
			channelTypes = append(channelTypes, uint32(v))
		} else {
			log.Fatal("Can't convert channels ", *channel)
		}
	}

	srv, rerr := sxutil.RegisterNode(*nodesrv, fmt.Sprintf("ChannelXband[%s]", *channel), channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		srv = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", srv)

	//	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(srv)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	// we need to add clients for each channel:
	pcClients := map[uint32]*sxutil.SXServiceClient{}

	for _, chnum := range channelTypes {
		argJson := fmt.Sprintf("{ChannelXband[%d]}", chnum)
		pcClients[chnum] = sxutil.NewSXServiceClient(client, chnum, argJson)
	}

	if sendfile != "" {

	} else if *all { // send all file
		sendAllStoredFile(pcClients)
	} else if *dir != "" {
	}

	//	wg.Wait()

}
