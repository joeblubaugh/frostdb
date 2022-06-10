package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/arcticdb"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query"
	"github.com/polarsignals/arcticdb/query/logicalplan"
	"github.com/segmentio/parquet-go"
)

func main() {

	pts := []State{
		{
			time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			"alerting",
			map[string]string{"foo": "bar"},
		},
		{
			time.Date(2020, 1, 1, 0, 1, 0, 0, time.UTC),
			"alerting",
			map[string]string{"foo": "bar"},
		},
		{
			time.Date(2020, 1, 1, 0, 2, 0, 0, time.UTC),
			"alerting",
			map[string]string{"foo": "bar"},
		},
		{
			time.Date(2020, 1, 1, 0, 3, 0, 0, time.UTC),
			"alerting",
			map[string]string{"foo": "bar"},
		},
		{
			time.Date(2020, 1, 1, 0, 4, 0, 0, time.UTC),
			"alerting",
			map[string]string{"foo": "bar", "onefoo": "onebar"},
		},
		{
			time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC),
			"silenced",
			map[string]string{"onefoo": "bar"},
		},
		{
			time.Date(2020, 1, 1, 2, 0, 0, 0, time.UTC),
			"pending",
			map[string]string{"onefoo": "bar"},
		},
	}

	s, err := NewDB()
	if err != nil {
		panic(err)
	}
	s.Store(context.Background(), pts...)

	// Print all points:
	eng := query.NewEngine(memory.DefaultAllocator, s.db.TableProvider())
	plan := eng.ScanTable("state").
		Filter(
			logicalplan.Col("state").Eq(logicalplan.Literal("alerting")),
		).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("state"),
			logicalplan.Col("labels.foo"),
			logicalplan.Col("labels.onefoo"),
		)

	err = plan.Execute(context.Background(), func(row arrow.Record) error {
		fmt.Println(row.Columns())
		return nil
	})

	fmt.Printf("\n\n\n\n")

	plan = eng.ScanTable("state").
		Filter(
			// Closest thing to "is set" that I can figure
			logicalplan.Col("labels.onefoo").NotEq(logicalplan.Literal("")),
		).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("state"),
			logicalplan.Col("labels.onefoo"),
		)

	err = plan.Execute(context.Background(), func(row arrow.Record) error {
		fmt.Println(row.Columns())
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
}

type StateDB struct {
	store *arcticdb.ColumnStore
	db    *arcticdb.DB
	table *arcticdb.Table

	schema *dynparquet.Schema
}

func NewDB() (*StateDB, error) {
	store := arcticdb.New(nil, 8192, 1*1024*1024).WithStoragePath(".")
	db, err := store.DB("state")
	if err != nil {
		return nil, err
	}

	schema := dynparquet.NewSchema(
		"state_schema",

		[]dynparquet.ColumnDefinition{
			{
				Name:          "timestamp",
				StorageLayout: parquet.Int(64),
				Dynamic:       false,
			},
			{
				Name:          "state",
				StorageLayout: parquet.String(),
				Dynamic:       false,
			},
			{
				Name:          "labels",
				StorageLayout: parquet.Encoded(parquet.Optional(parquet.String()), &parquet.RLEDictionary),
				Dynamic:       true,
			},
		},
		[]dynparquet.SortingColumn{
			dynparquet.Descending("timestamp"),
		},
	)
	logger := level.NewFilter(log.NewLogfmtLogger(os.Stdout), level.AllowAll())
	table, err := db.Table(
		"state",
		arcticdb.NewTableConfig(schema),
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
	State     string
	Labels    map[string]string
}

func (s *StateDB) Store(ctx context.Context, points ...State) error {
	// BUG: Someting wrong with nil safety for labels.

	// Insert the points into the database. We do this one at a time because the parquet rules for repetition level are beyone my expertise. Need to look at how Parca does this.
	for _, pt := range points {
		labels := map[string][]string{}
		keys := []string{}
		for k, v := range pt.Labels {
			labels[k] = append(labels[k], v)
			keys = append(keys, k)
		}
		sort.StringSlice(keys).Sort()

		buf, err := s.schema.NewBuffer(map[string][]string{"labels": keys})
		if err != nil {
			return err
		}

		row := parquet.Row{}
		labelCount := 0
		for _, k := range keys {
			if value, ok := pt.Labels[k]; ok {
				fmt.Println("key", k, "value", value)
				row = append(row, parquet.ValueOf(value).Level(0, 1, labelCount))
				labelCount++
			}
		}

		row = append(row, parquet.ValueOf(pt.State).Level(0, 0, labelCount))
		row = append(row, parquet.ValueOf(pt.Timestamp.UnixMilli()).Level(0, 0, labelCount+1))

		fmt.Println("row", row)
		wrote, err := buf.WriteRows([]parquet.Row{row})
		if err != nil {
			return err
		}

		fmt.Println("points", wrote)

		txID, err := s.table.InsertBuffer(ctx, buf)
		if err != nil {
			return err
		}

		fmt.Println("txID", txID)

	}

	return nil
}
