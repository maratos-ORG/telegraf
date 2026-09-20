package postgresql_multi

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/postgresql"
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
		IgnoredColumns:     []string{"stats_reset"},
	}
}

func TestInitDefaults(t *testing.T) {
	p := newPlugin()
	p.Query = []query{{Sqlquery: "SELECT 1"}}
	require.NoError(t, p.Init())

	require.Equal(t, config.Duration(60*time.Second), p.Timeout)
	require.Equal(t, config.Duration(5*time.Minute), p.DatnameRefreshInterval)
	require.Equal(t, 1, p.MaxConnections)
	require.Equal(t, "any", p.Role)

	q := p.Query[0]
	require.Equal(t, "postgresql", q.Measurement)
	require.Equal(t, "any", q.Role)
	require.False(t, q.stringTags)
	require.True(t, q.numericFloat)
	require.True(t, q.ignoredColumns["stats_reset"])
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
			IgnoredColumns:      []string{"query"},
		},
	}
	require.NoError(t, p.Init())

	inherited := p.Query[0]
	require.Equal(t, "primary", inherited.Role)
	require.True(t, inherited.stringTags)
	require.True(t, inherited.numericFloat)
	require.False(t, inherited.ignoredColumns["query"])

	overridden := p.Query[1]
	require.Equal(t, "replica", overridden.Role)
	require.False(t, overridden.stringTags)
	require.False(t, overridden.numericFloat)
	require.True(t, overridden.ignoredColumns["query"])
	require.True(t, overridden.ignoredColumns["stats_reset"])
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
			name:    "null and ignored columns are skipped",
			query:   query{IgnoredColumns: []string{"secret"}},
			columns: []string{"stats_reset", "secret", "nothing", "value"},
			row:     fakeRow{fields: []interface{}{time.Now(), "hidden", nil, int64(42)}},
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

func TestGatherClosesStaleServices(t *testing.T) {
	p := newPlugin()
	require.NoError(t, p.Init())

	p.dbServices["gone"] = &postgresql.Service{}
	p.dbServices["kept"] = &postgresql.Service{}
	p.closeStaleServices([]string{"kept"})

	require.Contains(t, p.dbServices, "kept")
	require.NotContains(t, p.dbServices, "gone")
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
		IgnoredColumns: []string{"stats_reset"},
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

func TestConcurrentQueriesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	server := startServer(t, "app_1", "app_2")
	defer server.stop()

	acc := server.run(t, &Postgresql{
		NumericAsFloat: true,
		DatnameInclude: []string{"app_.*"},
		MaxConnections: 2,
		Query: []query{
			{Measurement: "first", Sqlquery: "SELECT pg_sleep(0.2), 1 AS value"},
			{Measurement: "second", Sqlquery: "SELECT pg_sleep(0.2), 2 AS value"},
		},
	})
	require.Empty(t, acc.Errors)

	expected := map[string]int{"app_1": 1, "app_2": 1}
	require.Equal(t, expected, dbTags(acc.GetTelegrafMetrics(), "first"))
	require.Equal(t, expected, dbTags(acc.GetTelegrafMetrics(), "second"))
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
