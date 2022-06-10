package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/polarsignals/arcticdb"
	"github.com/polarsignals/arcticdb/dynparquet"
	"github.com/polarsignals/arcticdb/query"
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
			time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC),
			"state",
			map[string]string{"onefoo": "bar"},
		},
		{
			time.Date(2020, 1, 1, 2, 0, 0, 0, time.UTC),
			"state",
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
	err = eng.ScanTable("state").Execute(context.Background(), func(row arrow.Record) error {
		fmt.Println(row)
		return nil
	})
	if err != nil {
		panic(err)
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
				StorageLayout: parquet.Optional(parquet.String()),
				Dynamic:       true,
			},
		},
		[]dynparquet.SortingColumn{
			dynparquet.Descending("timestamp"),
		},
	)

	table, err := db.Table(
		"state",
		arcticdb.NewTableConfig(schema),
		log.NewNopLogger(),
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
	// First, generate a buffer from the schema for these points.
	labels := map[string][]string{}
	unsortedK := []string{}
	for _, pt := range points {
		for k, v := range pt.Labels {
			labels[k] = append(labels[k], v)
			unsortedK = append(unsortedK, k)
		}
	}

	fmt.Println("unsortedK", unsortedK)

	sort.StringSlice(unsortedK).Sort()
	// Dedup
	keys := []string{}
	j := -1
	for i := range unsortedK {
		if j < 0 {
			keys = append(keys, unsortedK[i])
			j = 0
			continue
		}

		if unsortedK[i] != keys[j] {
			keys = append(keys, unsortedK[i])
			j++
		}
	}

	fmt.Println("keys", keys)

	// Insert the points into the database. We do this one at a time because the parquet rules for repetition level are beyone my expertise. Need to look at how Parca does this.
	for _, pt := range points {
		buf, err := s.schema.NewBuffer(map[string][]string{"labels": keys})
		if err != nil {
			return err
		}
		row := parquet.Row{}

		row = append(row, parquet.ValueOf(pt.Timestamp.UnixMilli()).Level(0, 0, 0))
		row = append(row, parquet.ValueOf(pt.State).Level(0, 0, 1))

		for i, k := range keys {
			rep := 0
			if i > 0 {
				rep = 2
			}
			if value, ok := pt.Labels[k]; ok {
				fmt.Println("key", k, "value", value)
				row = append(row, parquet.ValueOf(value).Level(rep, 1, 2+i))
			}
		}

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
