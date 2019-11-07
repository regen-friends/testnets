package src

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	"text/tabwriter"

	"github.com/regen-network/testnets/util/uptime/db"
	"github.com/spf13/viper"
	"gopkg.in/mgo.v2/bson"
)

var (
	elChocoStartBlock     int64
	elChocoEndBlock       int64
	elChocoPointsPerBlock int64

	amazonasStartBlock     int64
	amazonasEndBlock       int64
	amazonasPointsPerBlock int64

	nodeRewards int64
)

type handler struct {
	db db.DB
}

func New(db db.DB) handler {
	return handler{db}
}

func GenerateAggregateQuery(startBlock int64, endBlock int64,
	elChocoStartBlock int64, elChocoEndBlock int64, amazonasStartBlock int64, amazonasEndBlock int64) []bson.M {

	aggQuery := []bson.M{}

	//Query for filtering blocks in between given start block and end block
	matchQuery := bson.M{
		"$match": bson.M{
			"$and": []bson.M{
				bson.M{
					"height": bson.M{"$gte": startBlock},
				},
				bson.M{
					"height": bson.M{"$lte": endBlock},
				},
			},
		},
	}

	aggQuery = append(aggQuery, matchQuery)

	//Query for Unwind the Array of validators from each block
	unwindQuery := bson.M{
		"$unwind": "$validators",
	}

	aggQuery = append(aggQuery, unwindQuery)

	//Query for calculating uptime count, upgrade1 count and upgrade2 count
	groupQuery := bson.M{
		"$group": bson.M{
			"_id":          "$validators",
			"uptime_count": bson.M{"$sum": 1},
			"upgrade1_block": bson.M{
				"$sum": bson.M{
					"$cond": []interface{}{
						bson.M{
							"$and": []bson.M{
								bson.M{"$gte": []interface{}{"$height", elChocoStartBlock}},
								bson.M{"$lte": []interface{}{"$height", elChocoEndBlock}},
							},
						},
						1,
						0,
					},
				},
			},
			"upgrade2_block": bson.M{
				"$sum": bson.M{
					"$cond": []interface{}{
						bson.M{
							"$and": []bson.M{
								bson.M{"$gte": []interface{}{"$height", amazonasStartBlock}},
								bson.M{"$lte": []interface{}{"$height", amazonasEndBlock}},
							},
						},
						1,
						0,
					},
				},
			},
		},
	}

	aggQuery = append(aggQuery, groupQuery)

	//Query for getting moniker, operator address from validators
	lookUpQuery := bson.M{
		"$lookup": bson.M{
			"from": "validators",
			"let":  bson.M{"id": "$_id"},
			"pipeline": []bson.M{
				bson.M{
					"$match": bson.M{
						"$expr": bson.M{"$eq": []string{"$address", "$$id"}},
					},
				},
				bson.M{
					"$project": bson.M{
						"description.moniker": 1, "operator_address": 1, "address": 1, "_id": 0,
					},
				},
			},
			"as": "validator_details",
		},
	}

	aggQuery = append(aggQuery, lookUpQuery)

	return aggQuery
}

// CalculateUpgradePoints - Calculates upgrade points by using upgrade points per block,
// upgrade block and end block height
func CalculateUpgradePoints(upgradePointsPerBlock int64, upgradeBlock int64, endBlockHeight int64) int64 {
	if upgradeBlock == 0 {
		return 0
	}
	points := upgradePointsPerBlock * (endBlockHeight - upgradeBlock + 1)

	return points
}

func (h handler) CalculateUptime(startBlock int64, endBlock int64) {
	//Read node rewards from config
	nodeRewards = viper.Get("node_rewards").(int64)

	// Read El Choco upgrade configs
	elChocoStartBlock = viper.Get("el_choco_startblock").(int64) + 1 //Need to consider votes from next block after upgrade
	elChocoEndBlock = viper.Get("el_choco_endblock").(int64) + 1
	elChocoPointsPerBlock = viper.Get("el_choco_reward_points_per_block").(int64)

	// Read Amazonas upgrade configs
	amazonasStartBlock = viper.Get("amazonas_startblock").(int64) + 1 //Need to consider votes from next block after upgrade
	amazonasEndBlock = viper.Get("amazonas_endblock").(int64) + 1
	amazonasPointsPerBlock = viper.Get("amazonas_reward_points_per_block").(int64)

	var validatorsList []ValidatorInfo //Intializing validators uptime

	fmt.Println("Fetching blocks from:", startBlock, ", to:", endBlock)

	aggQuery := GenerateAggregateQuery(startBlock, endBlock, elChocoStartBlock,
		elChocoEndBlock, amazonasStartBlock, amazonasEndBlock)

	results, err := h.db.QueryValAggregateData(aggQuery)

	if err != nil {
		fmt.Printf("Error while fetching validator data %v", err)
		db.HandleError(err)
	}

	for _, obj := range results {
		valInfo := ValidatorInfo{
			ValAddress: obj.Validator_details[0].Address,
			Info: Info{
				OperatorAddr:   obj.Validator_details[0].Operator_address,
				Moniker:        obj.Validator_details[0].Description.Moniker,
				UptimeCount:    obj.Uptime_count,
				Upgrade1Points: obj.Upgrade1_block,
				Upgrade2Points: obj.Upgrade2_block,
			},
		}

		validatorsList = append(validatorsList, valInfo)
	}

	//calculating uptime points
	for i, v := range validatorsList {
		uptime := float64(v.Info.UptimeCount) / (float64(endBlock) - float64(startBlock))
		uptimePoints := uptime * 300
		validatorsList[i].Info.UptimePoints = uptimePoints
		validatorsList[i].Info.TotalPoints = float64(validatorsList[i].Info.Upgrade1Points) +
			float64(validatorsList[i].Info.Upgrade2Points) + uptimePoints + float64(nodeRewards)
	}

	//Printing Uptime results in tabular view
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 0, ' ', tabwriter.Debug)
	fmt.Fprintln(w, " Operator Addr \t Moniker\t Uptime Count "+
		"\t Upgrade1 points \t Upgrade2 points \t Uptime points \t Node points \t Total points")

	for _, data := range validatorsList {
		var address string = data.Info.OperatorAddr

		//Assigning validator address if operator address is not found
		if address == "" {
			address = data.ValAddress + " (Hex Address)"
		}

		fmt.Fprintln(w, " "+address+"\t "+data.Info.Moniker+
			"\t  "+strconv.Itoa(int(data.Info.UptimeCount))+"\t "+strconv.Itoa(int(data.Info.Upgrade1Points))+
			" \t"+strconv.Itoa(int(data.Info.Upgrade2Points))+" \t"+fmt.Sprintf("%f", data.Info.UptimePoints)+
			"\t"+strconv.Itoa(int(nodeRewards))+"\t"+fmt.Sprintf("%f", data.Info.TotalPoints))
	}

	w.Flush()

	//Export data to csv file
	ExportToCsv(validatorsList, nodeRewards)
}

// ExportToCsv - Export data to CSV file
func ExportToCsv(data []ValidatorInfo, nodeRewards int64) {
	Header := []string{
		"ValOper Address", "Moniker", "Uptime Count", "elChoco Points",
		"Upgrade2 Points", "Uptime Points", "Node points",
	}

	file, err := os.Create("result.csv")

	if err != nil {
		log.Fatal("Cannot write to file", err)
	}

	defer file.Close() //Close file

	writer := csv.NewWriter(file)

	defer writer.Flush()

	//Write header titles
	_ = writer.Write(Header)

	for _, record := range data {
		var address string = record.Info.OperatorAddr

		//Assigning validator address if operator address is not found
		if address == "" {
			address = record.ValAddress + " (Hex Address)"
		}

		uptimeCount := strconv.Itoa(int(record.Info.UptimeCount))
		up1Points := strconv.Itoa(int(record.Info.Upgrade1Points))
		up2Points := strconv.Itoa(int(record.Info.Upgrade2Points))
		uptimePoints := fmt.Sprintf("%f", record.Info.UptimePoints)
		nodePoints := strconv.Itoa(int(nodeRewards))
		totalPoints := fmt.Sprintf("%f", record.Info.TotalPoints)
		addrObj := []string{address, record.Info.Moniker, uptimeCount, up1Points,
			up2Points, uptimePoints, nodePoints, totalPoints}
		err := writer.Write(addrObj)

		if err != nil {
			log.Fatal("Cannot write to file", err)
		}
	}
}
