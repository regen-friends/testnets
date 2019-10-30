package db

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/viper"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//configuring db name and collections
var (
	DB_NAME, dbErr        = viper.Get("database").(string)
	BLOCKS_COLLECTION     = "blocks"
	VALIDATORS_COLLECTION = "validators"
)

type Uptimes struct {
	ID            string        `json:"_id" bson:"_id"`
	UptimeCount   int64         `json:"uptimeCount" bson:"uptime_count"`
	Upgrade1Block UpgradeBlock  `json:"upgrade1Block" bson:"upgrade1_block"`
	Upgrade2Block UpgradeBlock2 `json:"upgrade2Block" bson:"upgrade2_block"`
}

type UpgradeBlock struct {
	Height int64 `json: "height" bson: "height"`
}

type UpgradeBlock2 struct {
	Height int64 `json: "blockheight" bson: "blockheight"`
}

type Blocks struct {
	ID         string   `json:"_id" bson:"_id"`
	Height     int64    `json:"height" bson:"height"`
	Validators []string `json:"validators" bson:"validators"`
}

type Validator struct {
	Address         string      `json:"address" bson:"address"`
	OperatorAddress string      `json:"operatorAddress" bson:"operator_address"`
	Description     Description `json:"description" bson:"description"`
}

type Description struct {
	Moniker string `json:"moniker" bson:"moniker"`
}

// Connect returns a pointer to a MongoDB instance,
// which is used for collecting the metrics required for uptime calculations
func Connect(info *mgo.DialInfo) (DB, error) {
	session, err := mgo.DialWithInfo(info)

	return Store{session: session}, err
}

// Terminate should be used to terminate a database session, generally in a defer statement inside main app file.
func (db Store) Terminate() {
	db.session.Close()
}

// FetchBlocks read the blocks data
func (db Store) FetchBlocks(startBlock int64, endBlock int64) ([]Blocks, error) {
	var blocks []Blocks

	andQuery := bson.M{"height": bson.M{"$gte": startBlock, "$lte": endBlock}}

	err := db.session.DB(DB_NAME).C(BLOCKS_COLLECTION).Find(andQuery).Sort("height").All(&blocks)

	return blocks, err
}

type m bson.M // just for brevity, bson.M type is map[string]interface{}

// Note: bson.D type used in the sort step, defines a slice of elements, so the order is preserved (unlike a map)

// GetUptime get validators uptime data
func (db Store) GetValidatorsUptime(startBlock int64, endBlock int64, up1StartBlock int64,
	up1EndBlock int64, up2StartBlock int64, up2EndBlock int64) ([]Uptimes, error) {
	var uptimes []Uptimes

	/*
db.blocks.aggregate(
  [
   {"$match":{"$and":[{"height":{"$gte":0}},{"height":{"$lte":2000000}}]}},
   {"$unwind":"$validators"},
   {"$group":{"_id":"$validators","uptime_count":{"$sum":1},
       "upgrade1_block":{
           "$min":{
               "$cond":[{
                   "$and":[{"$gte":["$height",953628]},{"$lte":["$height",953828]}
                   ]},
                   "$height",null]
           }
         },
         "upgrade2_block":{
           "$min":{
               "$cond":[{
                   "$and":[{"$gte":["$height",1722051]},{"$lte":["$height",1722250]}
                   ]},
                   "$height",null]
           }
         }
       }
   },
   {"$lookup": {
       "from": "validators",
       "let": { "id": "$_id" },
       "pipeline": [
        { "$match": { "$expr": { "$eq": ["$address", "$$id"] }}},
        { "$project": { "description.moniker": 1, "delegator_address":1,"_id": 0 }}
       ],
       "as": "validator_details"
       }
   }
  ]
)
	*/
	pipeLine := []m{
		m{
			"$match": m{
				"$and": []m{
					m{"height": m{"$gte": startBlock}},
					m{"height": m{"$lte": endBlock}},
				},
			},
		},
		m{
			"$unwind": "$validators",
		},
		m{
			"$group": m{
				"_id":          "$validators",
				"uptime_count": m{"$sum": 1},
				// "upgrade1_block": m{
				// 	"$min": m{
				// 		"$cond": []m{
				// 			m{
				// 				"$and": []m{
				// 					m{"$gte": []interface{}{"$height", up1StartBlock}},
				// 					m{"$lte": []interface{}{"$height", up1EndBlock}},
				// 				},
				// 			},
				// 			m{
				// 				"height": "$height",
				// 			},
				// 			nil,
				// 		},
				// 	},
				// },
				// "upgrade2_block": m{
				// 	"$min": m{
				// 		"$cond": []m{
				// 			m{
				// 				"$and": []m{
				// 					m{"$gte": []interface{}{"$height", up2StartBlock}},
				// 					m{"$lte": []interface{}{"$height", up2EndBlock}},
				// 				},
				// 			},
				// 			m{
				// 				"blockheight": "$height",
				// 			},
				// 			nil,
				// 		},
				// 	},
				// },
			},
		},
	}

	jsonString, _ := json.Marshal(pipeLine)
	fmt.Printf("mgo query: %s\n", jsonString)

	pipe := db.session.DB(DB_NAME).C(BLOCKS_COLLECTION).Pipe(pipeLine)

	err := pipe.All(&uptimes)

	fmt.Println(uptimes)

	return uptimes, err
}

//Get block by height
func (db Store) GetBlockByHeight(query bson.M) (Blocks, error) {
	var block Blocks
	err := db.session.DB(DB_NAME).C(BLOCKS_COLLECTION).Find(query).One(&block)
	return block, err
}

//GetValidator Read single validator info
func (db Store) GetValidator(query bson.M) (Validator, error) {
	var val Validator
	err := db.session.DB(DB_NAME).C(VALIDATORS_COLLECTION).Find(query).One(&val)

	return val, err
}

type (
	// DB interface defines all the methods accessible by the application
	DB interface {
		Terminate()
		GetValidatorsUptime(startBlock int64, endBlock int64, up1StartBlock int64,
			up1EndBlock int64, up2StartBlock int64, up2EndBlock int64) ([]Uptimes, error)
		FetchBlocks(startBlock int64, endBlock int64) ([]Blocks, error)
		GetValidator(query bson.M) (Validator, error)
		GetBlockByHeight(query bson.M) (Blocks, error)
	}

	// Store will be used to satisfy the DB interface
	Store struct {
		session *mgo.Session
	}
)