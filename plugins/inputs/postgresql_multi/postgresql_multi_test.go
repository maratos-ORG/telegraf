package postgresql_multi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/postgresql"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/testutil"
)

func newPlugin() *Postgresql {
	return &Postgresql{
		Log: testutil.Logger{},
		Config: postgresql.Config{
			Address:       config.NewSecret(nil),
			OutputAddress: "server",
			MaxIdle:       1,
			MaxOpen:       1,
		},
		PreparedStatements: true,
		NumericAsFloat:     true,
	}
}

func TestInitDefaults(t *testing.T) {
	p := newPlugin()
	p.Query = []query{{Sqlquery: "SELECT 1"}}
	require.NoError(t, p.Init())

	require.Zero(t, p.Timeout)
	require.Zero(t, p.MetadataRefreshInterval)
	require.Equal(t, 1, p.MaxConnections)
	require.Equal(t, "any", p.Role)

	q := p.Query[0]
	require.Equal(t, "postgresql", q.Measurement)
	require.Equal(t, "any", q.Role)
	require.False(t, q.stringTags)
	require.True(t, q.numericFloat)
}

// TestRegisteredDefaults pins the defaults an existing postgresql_extensible
// configuration relies on: no timeout, no caching, numeric columns as
// strings and the connection kept between collections.
func TestRegisteredDefaults(t *testing.T) {
	creator, found := inputs.Inputs["postgresql_multi"]
	require.True(t, found)

	p, ok := creator().(*Postgresql)
	require.True(t, ok)

	require.Zero(t, p.Timeout)
	require.Zero(t, p.MetadataRefreshInterval)
	require.Zero(t, p.MaxConnections)
	require.Zero(t, p.Role)
	require.False(t, p.NumericAsFloat)
	require.False(t, p.StringColumnsAsTags)
	require.True(t, p.KeepDatabaseConnections)
	require.True(t, p.PreparedStatements)
	require.Equal(t, 1, p.MaxIdle)
	require.Equal(t, 1, p.MaxOpen)
}

func TestInitDeprecatedOptions(t *testing.T) {
	tests := []struct {
		name      string
		databases []string
		query     query
		expected  string
	}{
		{
			name:     "version replaces min_version",
			query:    query{Sqlquery: "SELECT 1", Version: 901},
			expected: "SELECT 1",
		},
		{
			name:     "withdbname without databases",
			query:    query{Sqlquery: "SELECT * FROM pg_stat_database WHERE datname", Withdbname: true},
			expected: "SELECT * FROM pg_stat_database WHERE datname is not null",
		},
		{
			name:      "withdbname with databases",
			databases: []string{"app_1", "app_2"},
			query:     query{Sqlquery: "SELECT * FROM pg_stat_database WHERE datname", Withdbname: true},
			expected:  "SELECT * FROM pg_stat_database WHERE datname IN ('app_1','app_2')",
		},
		{
			name:      "databases without withdbname",
			databases: []string{"app_1"},
			query:     query{Sqlquery: "SELECT 1"},
			expected:  "SELECT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newPlugin()
			p.Databases = tt.databases
			p.Query = []query{tt.query}
			require.NoError(t, p.Init())

			require.Equal(t, tt.expected, p.Query[0].Sqlquery)
			if tt.query.Version != 0 {
				require.Equal(t, tt.query.Version, p.Query[0].MinVersion)
			}
		})
	}
}

func TestInitQueryOverrides(t *testing.T) {
	p := newPlugin()
	p.Role = "primary"
	p.StringColumnsAsTags = true
	p.NumericAsFloat = true
	p.Query = []query{
		{Sqlquery: "SELECT 1"},
		{
			Sqlquery:            "SELECT 2",
			Role:                "replica",
			StringColumnsAsTags: boolPtr(false),
			NumericAsFloat:      boolPtr(false),
		},
	}
	require.NoError(t, p.Init())

	inherited := p.Query[0]
	require.Equal(t, "primary", inherited.Role)
	require.True(t, inherited.stringTags)
	require.True(t, inherited.numericFloat)

	overridden := p.Query[1]
	require.Equal(t, "replica", overridden.Role)
	require.False(t, overridden.stringTags)
	require.False(t, overridden.numericFloat)
}

func TestInitScript(t *testing.T) {
	p := newPlugin()
	p.Query = []query{{Script: "testdata/test.sql"}}
	require.NoError(t, p.Init())
	require.Equal(t, "SELECT 1 AS first\n", p.Query[0].Sqlquery)
}

func TestInitErrors(t *testing.T) {
	tests := []struct {
		name     string
		plugin   *Postgresql
		expected string
	}{
		{
			name: "invalid plugin role",
			plugin: func() *Postgresql {
				p := newPlugin()
				p.Role = "master"
				return p
			}(),
			expected: `invalid role "master"`,
		},
		{
			name: "invalid query role",
			plugin: func() *Postgresql {
				p := newPlugin()
				p.Query = []query{{Sqlquery: "SELECT 1", Role: "standby"}}
				return p
			}(),
			expected: `query 0: invalid role "standby"`,
		},
		{
			name: "invalid include regex",
			plugin: func() *Postgresql {
				p := newPlugin()
				p.DatnameInclude = []string{"app_("}
				return p
			}(),
			expected: `compiling datname_include "app_(" failed`,
		},
		{
			name: "invalid exclude regex",
			plugin: func() *Postgresql {
				p := newPlugin()
				p.DatnameExclude = []string{"[a-"}
				return p
			}(),
			expected: `compiling datname_exclude "[a-" failed`,
		},
		{
			name: "missing script",
			plugin: func() *Postgresql {
				p := newPlugin()
				p.Query = []query{{Script: "testdata/does_not_exist.sql"}}
				return p
			}(),
			expected: "no such file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plugin.Init()
			require.ErrorContains(t, err, tt.expected)
		})
	}
}

func TestMatchesDatname(t *testing.T) {
	tests := []struct {
		name     string
		include  []string
		exclude  []string
		datname  string
		expected bool
	}{
		{name: "no filters", datname: "app", expected: true},
		{name: "include exact", include: []string{"app"}, datname: "app", expected: true},
		{name: "include anchored", include: []string{"app"}, datname: "myapp", expected: false},
		{name: "include anchored suffix", include: []string{"app"}, datname: "app2", expected: false},
		{name: "include regex", include: []string{"app_.*"}, datname: "app_prod", expected: true},
		{name: "include regex no match", include: []string{"app_.*"}, datname: "billing", expected: false},
		{name: "include multiple", include: []string{"app_.*", "billing"}, datname: "billing", expected: true},
		{name: "exclude only", exclude: []string{".*_test"}, datname: "app_test", expected: false},
		{name: "exclude only no match", exclude: []string{".*_test"}, datname: "app_prod", expected: true},
		{name: "include and exclude", include: []string{"app_.*"}, exclude: []string{".*_test"}, datname: "app_test", expected: false},
		{name: "include and exclude pass", include: []string{"app_.*"}, exclude: []string{".*_test"}, datname: "app_prod", expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newPlugin()
			p.DatnameInclude = tt.include
			p.DatnameExclude = tt.exclude
			require.NoError(t, p.Init())
			require.Equal(t, tt.expected, p.matchesDatname(tt.datname))
		})
	}
}

func TestMatchesRole(t *testing.T) {
	require.True(t, matchesRole("any", false))
	require.True(t, matchesRole("any", true))
	require.True(t, matchesRole("primary", false))
	require.False(t, matchesRole("primary", true))
	require.False(t, matchesRole("replica", false))
	require.True(t, matchesRole("replica", true))
}

func TestMatchesVersion(t *testing.T) {
	tests := []struct {
		name     string
		query    query
		version  int
		expected bool
	}{
		{name: "no limits", query: query{}, version: 1500, expected: true},
		{name: "min version met", query: query{MinVersion: 901}, version: 1500, expected: true},
		{name: "min version not met", query: query{MinVersion: 1600}, version: 1500, expected: false},
		{name: "max version exclusive", query: query{MaxVersion: 1500}, version: 1500, expected: false},
		{name: "max version below", query: query{MaxVersion: 1600}, version: 1500, expected: true},
		{name: "unknown version", query: query{MinVersion: 901}, version: 0, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, matchesVersion(&tt.query, tt.version))
		})
	}
}

func TestAccRow(t *testing.T) {
	tests := []struct {
		name           string
		query          query
		columns        []string
		numeric        map[string]bool
		row            fakeRow
		expectedTags   map[string]string
		expectedFields map[string]interface{}
	}{
		{
			name:    "db tag from job",
			columns: []string{"cat"},
			row:     fakeRow{fields: []interface{}{"gato"}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
			},
			expectedFields: map[string]interface{}{"cat": "gato"},
		},
		{
			name:    "db tag from datname column",
			columns: []string{"datname", "cat"},
			row:     fakeRow{fields: []interface{}{"name", "gato"}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "name",
			},
			expectedFields: map[string]interface{}{"datname": "name", "cat": "gato"},
		},
		{
			name:    "non-string datname column is ignored",
			columns: []string{"datname", "cat"},
			row:     fakeRow{fields: []interface{}{1, "gato"}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
			},
			expectedFields: map[string]interface{}{"datname": 1, "cat": "gato"},
		},
		{
			name:    "null and stats_reset columns are skipped",
			columns: []string{"stats_reset", "nothing", "value"},
			row:     fakeRow{fields: []interface{}{time.Now(), nil, int64(42)}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
			},
			expectedFields: map[string]interface{}{"value": int64(42)},
		},
		{
			name:    "tagvalue columns become tags",
			query:   query{Tagvalue: "state,pid"},
			columns: []string{"state", "pid", "count"},
			row:     fakeRow{fields: []interface{}{"idle", int64(7), int64(3)}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
				"state":  "idle",
				"pid":    "7",
			},
			expectedFields: map[string]interface{}{"count": int64(3)},
		},
		{
			name:    "string columns as tags",
			query:   query{StringColumnsAsTags: boolPtr(true)},
			columns: []string{"relname", "raw", "count"},
			row:     fakeRow{fields: []interface{}{"users", []byte("bytes"), int64(3)}},
			expectedTags: map[string]string{
				"server":  "server",
				"db":      "mydb",
				"relname": "users",
				"raw":     "bytes",
			},
			expectedFields: map[string]interface{}{"count": int64(3)},
		},
		{
			name:    "string columns as fields",
			columns: []string{"relname", "raw"},
			row:     fakeRow{fields: []interface{}{"users", []byte("bytes")}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
			},
			expectedFields: map[string]interface{}{"relname": "users", "raw": "bytes"},
		},
		{
			name:    "numeric as float",
			columns: []string{"ratio", "text", "big", "nan", "bad"},
			numeric: map[string]bool{"ratio": true, "big": true, "nan": true, "bad": true},
			row:     fakeRow{fields: []interface{}{"10.5", "10.5", []byte("1e3"), "NaN", "abc"}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
			},
			expectedFields: map[string]interface{}{"ratio": 10.5, "text": "10.5", "big": 1000.0},
		},
		{
			name:    "numeric as string",
			query:   query{NumericAsFloat: boolPtr(false)},
			columns: []string{"ratio"},
			numeric: map[string]bool{"ratio": true},
			row:     fakeRow{fields: []interface{}{"10.5"}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
			},
			expectedFields: map[string]interface{}{"ratio": "10.5"},
		},
		{
			name:    "timestamp column",
			query:   query{Timestamp: "ts"},
			columns: []string{"ts", "value"},
			row:     fakeRow{fields: []interface{}{time.Date(1980, 7, 23, 0, 0, 0, 0, time.UTC), int64(1)}},
			expectedTags: map[string]string{
				"server": "server",
				"db":     "mydb",
			},
			expectedFields: map[string]interface{}{"value": int64(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newPlugin()
			tt.query.Sqlquery = "SELECT 1"
			tt.query.Measurement = "pgTEST"
			p.Query = []query{tt.query}
			require.NoError(t, p.Init())

			var acc testutil.Accumulator
			j := job{datname: "mydb", query: &p.Query[0]}
			now := time.Now()
			require.NoError(t, p.accRow(&acc, tt.row, tt.columns, tt.numeric, j, now))
			require.Len(t, acc.Metrics, 1)

			metric := acc.Metrics[0]
			require.Equal(t, "pgTEST", metric.Measurement)
			require.Equal(t, tt.expectedTags, metric.Tags)
			require.Equal(t, tt.expectedFields, metric.Fields)
			if tt.query.Timestamp != "" {
				require.Equal(t, time.Date(1980, 7, 23, 0, 0, 0, 0, time.UTC), metric.Time)
			} else {
				require.Equal(t, now, metric.Time)
			}
		})
	}
}

func TestMetadataCached(t *testing.T) {
	p := newPlugin()
	p.MetadataRefreshInterval = config.Duration(time.Hour)
	require.NoError(t, p.Init())

	// The pool is not started, so any query would panic on a nil database
	p.dbVersion = 1700
	p.inRecovery = true
	p.datnames = []string{"app_1"}
	p.metadataUpdated = time.Now()

	require.NoError(t, p.refreshMetadata(context.Background()))
	require.Equal(t, 1700, p.dbVersion)
	require.True(t, p.inRecovery)
	require.Equal(t, []string{"app_1"}, p.datnames)
}

func TestMetadataNotCachedByDefault(t *testing.T) {
	p := newPlugin()
	require.NoError(t, p.Init())
	require.Zero(t, p.MetadataRefreshInterval)

	// A zero interval makes every collection read the server information
	require.True(t, refreshDue(time.Now(), time.Now(), time.Duration(p.MetadataRefreshInterval)))
}

func TestRefreshDue(t *testing.T) {
	interval := 2 * time.Minute
	updated := time.Date(2026, 9, 20, 17, 46, 0, 50*int(time.Millisecond), time.UTC)

	tests := []struct {
		name     string
		updated  time.Time
		now      time.Time
		expected bool
	}{
		{name: "never updated", now: updated, expected: true},
		{name: "same tick", updated: updated, now: updated, expected: false},
		{name: "one interval later", updated: updated, now: updated.Add(time.Minute), expected: false},
		{
			name:     "boundary reached slightly earlier than the update time",
			updated:  updated,
			now:      time.Date(2026, 9, 20, 17, 48, 0, 20*int(time.Millisecond), time.UTC),
			expected: true,
		},
		{name: "boundary reached exactly", updated: updated, now: time.Date(2026, 9, 20, 17, 48, 0, 0, time.UTC), expected: true},
		{name: "just before boundary", updated: updated, now: time.Date(2026, 9, 20, 17, 47, 59, 0, time.UTC), expected: false},
		{name: "long overdue", updated: updated, now: updated.Add(time.Hour), expected: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, refreshDue(tt.updated, tt.now, interval))
		})
	}
}

type fakeRow struct {
	fields []interface{}
}

func (f fakeRow) Scan(dest ...interface{}) error {
	if len(f.fields) != len(dest) {
		return errors.New("nada matchy buddy")
	}

	for i, d := range dest {
		switch d := d.(type) {
		case *interface{}:
			*d = f.fields[i]
		default:
			return fmt.Errorf("bad type %T", d)
		}
	}
	return nil
}

func boolPtr(v bool) *bool {
	return &v
}

// Integration tests

type testServer struct {
	container *testutil.Container
	address   string
}

func startServer(t *testing.T, databases ...string) *testServer {
	servicePort := "5432"
	container := &testutil.Container{
		Image:        "postgres:alpine",
		ExposedPorts: []string{servicePort},
		Env: map[string]string{
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort(servicePort),
		),
	}
	require.NoError(t, container.Start(), "failed to start container")

	for _, db := range databases {
		code, _, err := container.Exec([]string{"psql", "-U", "postgres", "-c", "CREATE DATABASE " + db})
		require.NoError(t, err)
		require.Zero(t, code, "failed to create database %q", db)
	}

	return &testServer{
		container: container,
		address: fmt.Sprintf(
			"host=%s port=%s user=postgres dbname=postgres sslmode=disable",
			container.Address,
			container.Ports[servicePort],
		),
	}
}

func (s *testServer) stop() {
	s.container.Terminate()
}

func (s *testServer) run(t *testing.T, p *Postgresql) *testutil.Accumulator {
	p.Log = testutil.Logger{}
	p.Config.Address = config.NewSecret([]byte(s.address))
	if p.Config.MaxOpen == 0 {
		p.Config.MaxIdle = 1
		p.Config.MaxOpen = 1
	}
	require.NoError(t, p.Init())

	var acc testutil.Accumulator
	require.NoError(t, p.Start(&acc))
	defer p.Stop()
	require.NoError(t, p.Gather(&acc))

	return &acc
}

func dbTags(metrics []telegraf.Metric, measurement string) map[string]int {
	result := make(map[string]int)
	for _, m := range metrics {
		if m.Name() != measurement {
			continue
		}
		db, _ := m.GetTag("db")
		result[db]++
	}
	return result
}

func TestGeneratesMetricsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t)
	defer server.stop()

	acc := server.run(t, &Postgresql{
		NumericAsFloat: true,
		Query: []query{{
			Sqlquery:   "SELECT * FROM pg_stat_database WHERE datname = 'postgres'",
			MinVersion: 901,
		}},
	})
	require.Empty(t, acc.Errors)

	intMetrics := []string{
		"xact_commit",
		"xact_rollback",
		"blks_read",
		"blks_hit",
		"tup_returned",
		"tup_fetched",
		"tup_inserted",
		"tup_updated",
		"tup_deleted",
		"conflicts",
		"temp_files",
		"temp_bytes",
		"deadlocks",
		"numbackends",
		"datid",
	}
	for _, metric := range intMetrics {
		require.Truef(t, acc.HasInt64Field("postgresql", metric), "expected %s to be an integer", metric)
	}
	for _, metric := range []string{"blk_read_time", "blk_write_time"} {
		require.Truef(t, acc.HasFloatField("postgresql", metric), "expected %s to be a float", metric)
	}
	require.True(t, acc.HasStringField("postgresql", "datname"))
	require.False(t, acc.HasField("postgresql", "stats_reset"))
	require.Equal(t, "postgres", acc.TagValue("postgresql", "db"))
}

func TestMultipleDatabasesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t, "app_1", "app_2", "skip_me")
	defer server.stop()

	acc := server.run(t, &Postgresql{
		NumericAsFloat: true,
		DatnameInclude: []string{"app_.*", "postgres"},
		DatnameExclude: []string{"app_2"},
		Query: []query{{
			Measurement: "current",
			Sqlquery:    "SELECT current_database() AS name, 1 AS value",
		}},
	})
	require.Empty(t, acc.Errors)

	expected := map[string]int{"app_1": 1, "postgres": 1}
	require.Equal(t, expected, dbTags(acc.GetTelegrafMetrics(), "current"))

	// The db tag must reflect the database the query was run in
	for _, m := range acc.GetTelegrafMetrics() {
		db, _ := m.GetTag("db")
		name, _ := m.GetField("name")
		require.Equal(t, db, name)
	}
}

func TestConcurrentDatabasesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t, "app_1", "app_2", "app_3", "app_4")
	defer server.stop()
	server.address += " application_name=postgresql_multi_test"

	// Each query counts the active sessions of this plugin before sleeping,
	// so concurrently running queries see each other.
	const countActive = `SELECT (SELECT count(*) FROM pg_stat_activity ` +
		`WHERE application_name = 'postgresql_multi_test' AND state = 'active' AND datname <> 'postgres') AS active, ` +
		`pg_backend_pid() AS pid, pg_sleep(0.2)`

	tests := []struct {
		name           string
		maxConnections int
		expected       int64
	}{
		{name: "sequential", maxConnections: 1, expected: 1},
		{name: "two databases", maxConnections: 2, expected: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := server.run(t, &Postgresql{
				NumericAsFloat: true,
				DatnameInclude: []string{"app_.*"},
				MaxConnections: tt.maxConnections,
				Query: []query{
					{Measurement: "first", Sqlquery: countActive},
					{Measurement: "second", Sqlquery: countActive},
				},
			})
			require.Empty(t, acc.Errors)

			expectedDBs := map[string]int{"app_1": 1, "app_2": 1, "app_3": 1, "app_4": 1}
			require.Equal(t, expectedDBs, dbTags(acc.GetTelegrafMetrics(), "first"))
			require.Equal(t, expectedDBs, dbTags(acc.GetTelegrafMetrics(), "second"))

			// The maximum number of active sessions seen by any query must
			// match the number of concurrently processed databases
			var maxActive int64
			pids := make(map[string]map[int64]bool)
			for _, m := range acc.GetTelegrafMetrics() {
				v, found := m.GetField("active")
				require.True(t, found)
				if active := v.(int64); active > maxActive {
					maxActive = active
				}

				// Both queries of a database must have run on the same connection
				db, _ := m.GetTag("db")
				pid, _ := m.GetField("pid")
				if pids[db] == nil {
					pids[db] = make(map[int64]bool)
				}
				pids[db][pid.(int64)] = true
			}
			require.Equal(t, tt.expected, maxActive)
			for db, set := range pids {
				require.Lenf(t, set, 1, "database %s used more than one connection", db)
			}
		})
	}
}

func TestConcurrentQueriesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t, "app_1")
	defer server.stop()
	server.address += " application_name=postgresql_multi_test"

	// With a single database to query the queries run concurrently in it
	const countActive = `SELECT (SELECT count(*) FROM pg_stat_activity ` +
		`WHERE application_name = 'postgresql_multi_test' AND state = 'active') AS active, pg_sleep(0.2)`

	tests := []struct {
		name    string
		include []string
		db      string
	}{
		{name: "connection database", db: "postgres"},
		{name: "single filtered database", include: []string{"app_1"}, db: "app_1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := server.run(t, &Postgresql{
				NumericAsFloat: true,
				MaxConnections: 3,
				DatnameInclude: tt.include,
				Query: []query{
					{Measurement: "q1", Sqlquery: countActive},
					{Measurement: "q2", Sqlquery: countActive},
					{Measurement: "q3", Sqlquery: countActive},
					{Measurement: "q4", Sqlquery: countActive},
				},
			})
			require.Empty(t, acc.Errors)

			var maxActive int64
			for _, m := range acc.GetTelegrafMetrics() {
				db, _ := m.GetTag("db")
				require.Equal(t, tt.db, db)
				v, found := m.GetField("active")
				require.True(t, found)
				if active := v.(int64); active > maxActive {
					maxActive = active
				}
			}
			require.Equal(t, int64(3), maxActive)
		})
	}
}

func TestKeepConnectionsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t, "app_1", "app_2")
	defer server.stop()
	server.address += " application_name=postgresql_multi_test"

	sessions := func() int {
		code, out, err := server.container.Exec([]string{
			"psql", "-U", "postgres", "-tAc",
			"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'postgresql_multi_test'",
		})
		require.NoError(t, err)
		require.Zero(t, code)
		buf, err := io.ReadAll(out)
		require.NoError(t, err)
		// The output is prefixed with a docker stream header
		n, err := strconv.Atoi(strings.TrimFunc(string(buf), func(r rune) bool { return r < '0' || r > '9' }))
		require.NoError(t, err)
		return n
	}

	tests := []struct {
		name     string
		keep     bool
		include  []string
		expected int
	}{
		{name: "single database closed", keep: false, expected: 0},
		{name: "single database kept", keep: true, expected: 3},
		{name: "multiple databases closed", keep: false, include: []string{"app_.*"}, expected: 0},
		{name: "multiple databases kept", keep: true, include: []string{"app_.*"}, expected: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Postgresql{
				Log:                     testutil.Logger{},
				Config:                  postgresql.Config{Address: config.NewSecret([]byte(server.address)), MaxIdle: 1, MaxOpen: 1},
				NumericAsFloat:          true,
				MaxConnections:          3,
				KeepDatabaseConnections: tt.keep,
				DatnameInclude:          tt.include,
				Query: []query{
					{Measurement: "q1", Sqlquery: "SELECT pg_sleep(0.1), 1 AS v"},
					{Measurement: "q2", Sqlquery: "SELECT pg_sleep(0.1), 2 AS v"},
					{Measurement: "q3", Sqlquery: "SELECT pg_sleep(0.1), 3 AS v"},
				},
			}
			require.NoError(t, p.Init())

			var acc testutil.Accumulator
			require.NoError(t, p.Start(&acc))
			defer p.Stop()
			require.NoError(t, p.Gather(&acc))
			require.Empty(t, acc.Errors)

			// Give the server a moment to register closed sessions
			time.Sleep(500 * time.Millisecond)
			require.Equal(t, tt.expected, sessions())
		})
	}
}

func TestScriptIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t)
	defer server.stop()

	acc := server.run(t, &Postgresql{
		NumericAsFloat: true,
		Query: []query{{
			Measurement: "script",
			Script:      "testdata/test.sql",
		}},
	})
	require.Empty(t, acc.Errors)

	require.Equal(t, uint64(1), acc.NMetrics())
	require.True(t, acc.HasInt64Field("script", "first"))
}

func TestNumericConversionIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t)
	defer server.stop()

	acc := server.run(t, &Postgresql{
		NumericAsFloat: true,
		Query: []query{
			{Measurement: "converted", Sqlquery: "SELECT 10.5::numeric AS value"},
			{Measurement: "raw", Sqlquery: "SELECT 10.5::numeric AS value", NumericAsFloat: boolPtr(false)},
		},
	})
	require.Empty(t, acc.Errors)

	v, found := acc.FloatField("converted", "value")
	require.True(t, found)
	require.InDelta(t, 10.5, v, testutil.DefaultDelta)

	s, found := acc.StringField("raw", "value")
	require.True(t, found)
	require.Equal(t, "10.5", s)
}

func TestQueryTimeoutIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t)
	defer server.stop()

	acc := server.run(t, &Postgresql{
		NumericAsFloat: true,
		Query: []query{
			{Measurement: "slow", Sqlquery: "SELECT pg_sleep(5), 1 AS value", Timeout: config.Duration(500 * time.Millisecond)},
			{Measurement: "fast", Sqlquery: "SELECT 1 AS value"},
		},
	})

	require.Len(t, acc.Errors, 1)
	require.ErrorContains(t, acc.Errors[0], `query "slow"`)
	require.False(t, acc.HasMeasurement("slow"))
	require.True(t, acc.HasInt64Field("fast", "value"))
}

func TestCachedRoleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t)
	defer server.stop()

	p := &Postgresql{
		Log:                     testutil.Logger{},
		Config:                  postgresql.Config{Address: config.NewSecret([]byte(server.address)), MaxIdle: 1, MaxOpen: 1},
		NumericAsFloat:          true,
		MetadataRefreshInterval: config.Duration(time.Hour),
		Query:                   []query{{Measurement: "primary_only", Sqlquery: "SELECT 1 AS value", Role: "primary"}},
	}
	require.NoError(t, p.Init())

	var acc testutil.Accumulator
	require.NoError(t, p.Start(&acc))
	defer p.Stop()

	// The container is a primary, so the query runs
	require.NoError(t, p.Gather(&acc))
	require.Empty(t, acc.Errors)
	require.True(t, acc.HasMeasurement("primary_only"))

	// Pretend the server became a replica without the cache knowing about
	// it: the next collection must use the cached role instead of asking
	// the server again
	acc.ClearMetrics()
	p.inRecovery = true
	require.NoError(t, p.Gather(&acc))
	require.Empty(t, acc.Errors)
	require.False(t, acc.HasMeasurement("primary_only"))
}

func TestRoleFilterIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t)
	defer server.stop()

	// The container is always a primary
	acc := server.run(t, &Postgresql{
		NumericAsFloat: true,
		Query: []query{
			{Measurement: "primary_only", Sqlquery: "SELECT 1 AS value", Role: "primary"},
			{Measurement: "replica_only", Sqlquery: "SELECT 1 AS value", Role: "replica"},
			{Measurement: "everywhere", Sqlquery: "SELECT 1 AS value"},
		},
	})
	require.Empty(t, acc.Errors)

	require.True(t, acc.HasMeasurement("primary_only"))
	require.False(t, acc.HasMeasurement("replica_only"))
	require.True(t, acc.HasMeasurement("everywhere"))
}
