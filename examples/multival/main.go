package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore/filesystem"
)

func main() {

	// How big are 1 million evalutation events on disk? Let's find out!
	pts := genStates(1e5)

	s, err := NewDB()
	if err != nil {
		panic(err)
	}
	s.Store(context.Background(), pts...)

	// Print all points:
	//eng := query.NewEngine(memory.DefaultAllocator, s.db.TableProvider())
	//plan := eng.ScanTable("state").
	//	Filter(
	//		logicalplan.Col("state").Eq(logicalplan.Literal("alerting")),
	//	).
	//	Project(
	//		logicalplan.Col("timestamp"),
	//		logicalplan.Col("state"),
	//		logicalplan.Col("labels.foo"),
	//		logicalplan.Col("labels.onefoo"),
	//	)

	//err = plan.Execute(context.Background(), func(row arrow.Record) error {
	//	fmt.Println(row.Columns())
	//	return nil
	//})

	//fmt.Printf("\n\n\n\n")

	//plan = eng.ScanTable("state").
	//	Filter(
	//		// Closest thing to "is set" that I can figure
	//		logicalplan.Col("labels.onefoo").NotEq(logicalplan.Literal("")),
	//	).
	//	Project(
	//		logicalplan.Col("timestamp"),
	//		logicalplan.Col("state"),
	//		logicalplan.Col("labels.foo"),
	//		logicalplan.Col("labels.onefoo"),
	//	)

	//err = plan.Execute(context.Background(), func(row arrow.Record) error {
	//	fmt.Println(row.Columns())
	//	return nil
	//})

	fmt.Println(s.table.Schema())
	// Creates a new active block and writes down the current active block. Does this in the background.
	s.table.RotateBlock()
	s.db.Close()
	s.store.Close()

	fmt.Println("waiting for disk writes")
	<-time.Tick(time.Second * 5)
}

type StateDB struct {
	store *frostdb.ColumnStore
	db    *frostdb.DB
	table *frostdb.Table

	schema *dynparquet.Schema
}

func NewDB() (*StateDB, error) {
	bucket, err := filesystem.NewBucket("/Users/joe/workspace/polarsignals/arcticdb")
	if err != nil {
		return nil, err
	}

	store := frostdb.New(nil,
		8192,        // 8k granules - about 1 memory page.
		1*1024*1024, // 100 MiB active memory size - this is the buffered size, before we write to disk
	).WithStorageBucket(bucket)
	if err != nil {
		return nil, err
	}

	db, err := store.DB("state")
	if err != nil {
		return nil, err
	}

	schema := dynparquet.NewSchema(
		"state_schema",

		[]dynparquet.ColumnDefinition{
			{
				Name:          "labels",
				StorageLayout: parquet.Encoded(parquet.Optional(parquet.String()), &parquet.RLEDictionary),
				Dynamic:       true,
			},
			{
				Name:          "rule_uid",
				StorageLayout: parquet.String(),
				Dynamic:       false,
			},
			{
				Name:          "state",
				StorageLayout: parquet.String(),
				Dynamic:       false,
			},
			{
				Name:          "timestamp",
				StorageLayout: parquet.Int(64),
				Dynamic:       false,
			},
		},
		[]dynparquet.SortingColumn{
			dynparquet.Ascending("timestamp"),
			dynparquet.Ascending("rule_uid"),
		},
	)
	logger := level.NewFilter(log.NewLogfmtLogger(os.Stdout), level.AllowDebug())
	table, err := db.Table(
		"state",
		frostdb.NewTableConfig(schema),
		logger,
	)
	if err != nil {
		return nil, err
	}

	return &StateDB{
		store:  store,
		db:     db,
		table:  table,
		schema: schema,
	}, nil
}

type State struct {
	Timestamp time.Time
	// RuleID and string are "just" special labels, but we want to sort on them,
	// and the sort order of dynamic columns depends on when they're added to the schema.
	RuleID string
	State  string
	Labels map[string]string
}

var validStates []string = []string{
	"Alerting",
	"Pending",
	"Resolved",
}

const maxLabels int = 20

func randLabels(count int) map[string]string {
	labels := make(map[string]string, count)

	// We want *some* label overlap, so the label string is a padded integer within "maxLabels", sampled without replacement, and the value is a random value within 5*maxLabels, without replacement for each key.
	keys := []string{}
	for i := 0; i < maxLabels; i++ {
		keys = append(keys, fmt.Sprintf("%010d", i))
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i] = keys[j]
	})

	for i, k := range keys {
		if i > count {
			break
		}
		labels[k] = fmt.Sprintf("%015d", rand.Intn(5*maxLabels))
	}

	return labels
}

func genStates(count int) []State {
	states := make([]State, 0, count)
	for i := 0; i < count; i++ {
		state := State{
			Timestamp: time.UnixMilli(rand.Int63n(2592000000) + 1653271512000), // From 23ish May to 23 June
			RuleID:    fmt.Sprintf("%020d", rand.Intn(1e6))[0:7],
			State:     validStates[rand.Intn(len(validStates))],
			Labels:    randLabels(5 + rand.Intn(maxLabels-5)),
		}

		states = append(states, state)
	}
	return states
}

func (s *StateDB) Store(ctx context.Context, points ...State) error {

	for _, pt := range points {
		keys := []string{}
		for k := range pt.Labels {
			keys = append(keys, k)
		}
		sort.StringSlice(keys).Sort()

		row := parquet.Row{}
		labelCount := 0
		for _, k := range keys {
			if value, ok := pt.Labels[k]; ok {
				row = append(row, parquet.ValueOf(value).Level(0, 1, labelCount))
				labelCount++
			}
		}

		// Mark all the label keys we've seen. We need this to correctly set the repetition level for dynamic columns.
		row = append(row, parquet.ValueOf(pt.RuleID).Level(0, 0, labelCount))
		row = append(row, parquet.ValueOf(pt.State).Level(0, 0, labelCount+1))
		row = append(row, parquet.ValueOf(pt.Timestamp.UnixMilli()).Level(0, 0, labelCount+2))

		// We write one at a time here because it's complicated to set the
		// repetition level correctly for dynamic columns. You have to track which
		// columns have been seen in the current buffer and set the repetition
		// level to the correct depth. Wish arcticdb handled this, but c'est la
		// vie.
		buf, err := s.schema.NewBuffer(map[string][]string{"labels": keys})
		if err != nil {
			return err
		}

		_, err = buf.WriteRows([]parquet.Row{row})
		if err != nil {
			return err
		}

		_, err = s.table.InsertBuffer(ctx, buf)
		if err != nil {
			return err
		}
	}

	return nil
}
