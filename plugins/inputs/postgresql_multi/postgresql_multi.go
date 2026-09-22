//go:generate ../../../tools/readme_config_includer/generator
package postgresql_multi

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	// Required for SQL framework driver
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/common/postgresql"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//go:embed sample.conf
var sampleConfig string

type Postgresql struct {
	Query                   []query         `toml:"query"`
	PreparedStatements      bool            `toml:"prepared_statements"`
	Timeout                 config.Duration `toml:"timeout"`
	Role                    string          `toml:"role"`
	DatnameInclude          []string        `toml:"datname_include"`
	DatnameExclude          []string        `toml:"datname_exclude"`
	DatnameRefreshInterval  config.Duration `toml:"datname_refresh_interval"`
	MaxConnections          int             `toml:"max_connections"`
	KeepDatabaseConnections bool            `toml:"keep_database_connections"`
	StringColumnsAsTags     bool            `toml:"string_columns_as_tags"`
	NumericAsFloat          bool            `toml:"numeric_as_float"`
	IgnoredColumns          []string        `toml:"ignored_columns"`
	Log                     telegraf.Logger `toml:"-"`
	postgresql.Config

	service *postgresql.Service

	includeRegex []*regexp.Regexp
	excludeRegex []*regexp.Regexp

	datnames        []string
	datnamesUpdated time.Time
}

type query struct {
	Sqlquery            string          `toml:"sqlquery"`
	Script              string          `toml:"script"`
	MinVersion          int             `toml:"min_version"`
	MaxVersion          int             `toml:"max_version"`
	Tagvalue            string          `toml:"tagvalue"`
	Measurement         string          `toml:"measurement"`
	Timestamp           string          `toml:"timestamp"`
	Timeout             config.Duration `toml:"timeout"`
	Role                string          `toml:"role"`
	StringColumnsAsTags *bool           `toml:"string_columns_as_tags"`
	NumericAsFloat      *bool           `toml:"numeric_as_float"`
	IgnoredColumns      []string        `toml:"ignored_columns"`

	additionalTags map[string]bool
	ignoredColumns map[string]bool
	stringTags     bool
	numericFloat   bool
}

type scanner interface {
	Scan(dest ...interface{}) error
}

// job is a single unit of work: one query executed in one database
type job struct {
	datname string
	query   *query
}

func (*Postgresql) SampleConfig() string {
	return sampleConfig
}

func (p *Postgresql) Init() error {
	if p.Timeout <= 0 {
		p.Timeout = config.Duration(60 * time.Second)
	}
	if p.DatnameRefreshInterval <= 0 {
		p.DatnameRefreshInterval = config.Duration(5 * time.Minute)
	}
	if p.MaxConnections <= 0 {
		p.MaxConnections = 1
	}
	if err := validateRole(p.Role); err != nil {
		return err
	}
	if p.Role == "" {
		p.Role = "any"
	}

	// Compile the database name filters
	for _, pattern := range p.DatnameInclude {
		re, err := regexp.Compile("^(?:" + pattern + ")$")
		if err != nil {
			return fmt.Errorf("compiling datname_include %q failed: %w", pattern, err)
		}
		p.includeRegex = append(p.includeRegex, re)
	}
	for _, pattern := range p.DatnameExclude {
		re, err := regexp.Compile("^(?:" + pattern + ")$")
		if err != nil {
			return fmt.Errorf("compiling datname_exclude %q failed: %w", pattern, err)
		}
		p.excludeRegex = append(p.excludeRegex, re)
	}

	// Set defaults for the queries
	for i, q := range p.Query {
		if q.Sqlquery == "" {
			query, err := os.ReadFile(q.Script)
			if err != nil {
				return err
			}
			q.Sqlquery = string(query)
		}
		if q.Measurement == "" {
			q.Measurement = "postgresql"
		}
		if err := validateRole(q.Role); err != nil {
			return fmt.Errorf("query %d: %w", i, err)
		}
		if q.Role == "" {
			q.Role = p.Role
		}

		q.stringTags = p.StringColumnsAsTags
		if q.StringColumnsAsTags != nil {
			q.stringTags = *q.StringColumnsAsTags
		}
		q.numericFloat = p.NumericAsFloat
		if q.NumericAsFloat != nil {
			q.numericFloat = *q.NumericAsFloat
		}

		q.ignoredColumns = make(map[string]bool, len(p.IgnoredColumns)+len(q.IgnoredColumns))
		for _, col := range p.IgnoredColumns {
			q.ignoredColumns[col] = true
		}
		for _, col := range q.IgnoredColumns {
			q.ignoredColumns[col] = true
		}

		q.additionalTags = make(map[string]bool)
		if q.Tagvalue != "" {
			for _, tag := range strings.Split(q.Tagvalue, ",") {
				q.additionalTags[tag] = true
			}
		}
		p.Query[i] = q
	}
	p.Config.SimpleProtocol = !p.PreparedStatements

	// Create a service to access the PostgreSQL server
	service, err := p.Config.CreateService()
	if err != nil {
		return err
	}
	p.service = service

	return nil
}

func (p *Postgresql) Start(_ telegraf.Accumulator) error {
	if err := p.service.Start(); err != nil {
		return err
	}

	// Make sure the pool of the connection database allows the configured
	// number of concurrent queries and reuses them within a collection
	if p.MaxConnections > p.MaxOpen {
		p.service.DB.SetMaxOpenConns(p.MaxConnections)
	}
	if p.MaxConnections > p.MaxIdle {
		p.service.DB.SetMaxIdleConns(p.MaxConnections)
	}

	return nil
}

func (p *Postgresql) Gather(acc telegraf.Accumulator) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.Timeout))
	defer cancel()

	// Retrieving the database version and the server role
	var dbVersion int
	var inRecovery bool
	row := p.service.DB.QueryRowContext(ctx,
		`SELECT setting::integer / 100, pg_is_in_recovery() FROM pg_settings WHERE name = 'server_version_num'`)
	if err := row.Scan(&dbVersion, &inRecovery); err != nil {
		return fmt.Errorf("querying server version and role failed: %w", err)
	}

	// Select the queries to run for this server version and role
	queries := make([]*query, 0, len(p.Query))
	for i := range p.Query {
		q := &p.Query[i]
		if !matchesVersion(q, dbVersion) {
			continue
		}
		if !matchesRole(q.Role, inRecovery) {
			p.Log.Debugf("Skipping query %q as the server role does not match %q", q.Measurement, q.Role)
			continue
		}
		queries = append(queries, q)
	}
	if len(queries) == 0 {
		return nil
	}

	// set default timestamp to Now and use for all generated metrics during
	// the same Gather call
	timestamp := time.Now()

	// Unless the connections should be kept, close the idle connections of
	// the connection database after the collection
	if !p.KeepDatabaseConnections {
		defer p.closeIdleConnections()
	}

	// Determine the databases to run the queries in
	datnames := []string{p.service.ConnectionDatabase}
	if len(p.includeRegex) > 0 || len(p.excludeRegex) > 0 {
		if err := p.refreshDatnames(ctx); err != nil {
			acc.AddError(err)
		}
		datnames = p.datnames
	}
	if len(datnames) == 0 {
		return nil
	}

	// A single database gets all connections: run the queries concurrently
	if len(datnames) == 1 {
		if err := p.gatherInDatabase(ctx, acc, datnames[0], queries, timestamp, p.MaxConnections); err != nil {
			acc.AddError(fmt.Errorf("database %q: %w", datnames[0], err))
		}
		return nil
	}

	// Process up to max_connections databases concurrently, running the
	// queries of each database sequentially on a single connection which is
	// closed when done. The semaphore is acquired before spawning the
	// goroutine to avoid a large number of waiting goroutines when there are
	// many databases.
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, p.MaxConnections)
	for _, datname := range datnames {
		semaphore <- struct{}{}
		wg.Add(1)
		go func(datname string) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if err := p.gatherInDatabase(ctx, acc, datname, queries, timestamp, 1); err != nil {
				acc.AddError(fmt.Errorf("database %q: %w", datname, err))
			}
		}(datname)
	}
	wg.Wait()

	return nil
}

// gatherInDatabase runs the queries in the given database with up to
// concurrency queries executing at the same time. The connection database
// uses the existing pool, any other database gets a pool of its own which
// is closed when done.
func (p *Postgresql) gatherInDatabase(
	ctx context.Context,
	acc telegraf.Accumulator,
	datname string,
	queries []*query,
	timestamp time.Time,
	concurrency int,
) error {
	service := p.service
	if datname != p.service.ConnectionDatabase {
		var err error
		service, err = p.Config.CreateServiceForDatabase(datname)
		if err != nil {
			return err
		}
		if err := service.Start(); err != nil {
			return err
		}
		defer service.Stop()
		service.DB.SetMaxOpenConns(concurrency)
		service.DB.SetMaxIdleConns(concurrency)
	}

	p.gatherDatabase(ctx, acc, service, datname, queries, timestamp, concurrency)
	return nil
}

// closeIdleConnections closes the idle connections of the connection
// database by temporarily dropping the idle limit of the pool
func (p *Postgresql) closeIdleConnections() {
	p.service.DB.SetMaxIdleConns(0)
	p.service.DB.SetMaxIdleConns(max(p.MaxIdle, p.MaxConnections))
}

// gatherDatabase runs the queries in one database with the given number of
// queries executing concurrently
func (p *Postgresql) gatherDatabase(
	ctx context.Context,
	acc telegraf.Accumulator,
	service *postgresql.Service,
	datname string,
	queries []*query,
	timestamp time.Time,
	concurrency int,
) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrency)
	for _, q := range queries {
		semaphore <- struct{}{}
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if err := p.gatherMetricsFromQuery(ctx, acc, service, j, timestamp); err != nil {
				acc.AddError(fmt.Errorf("database %q: %w", j.datname, err))
			}
		}(job{datname: datname, query: q})
	}
	wg.Wait()
}

func (p *Postgresql) Stop() {
	p.service.Stop()
}

// refreshDatnames updates the list of databases matching the filters if a
// refresh is due
func (p *Postgresql) refreshDatnames(ctx context.Context) error {
	if !refreshDue(p.datnamesUpdated, time.Now(), time.Duration(p.DatnameRefreshInterval)) {
		return nil
	}

	rows, err := p.service.DB.QueryContext(ctx,
		`SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY datname`)
	if err != nil {
		return fmt.Errorf("querying database list failed: %w", err)
	}
	defer rows.Close()

	datnames := make([]string, 0)
	for rows.Next() {
		var datname string
		if err := rows.Scan(&datname); err != nil {
			return fmt.Errorf("scanning database list failed: %w", err)
		}
		if p.matchesDatname(datname) {
			datnames = append(datnames, datname)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading database list failed: %w", err)
	}

	if len(datnames) == 0 {
		p.Log.Warn("No database matches the datname_include/datname_exclude filters")
	}
	p.datnames = datnames
	p.datnamesUpdated = time.Now()

	return nil
}

func (p *Postgresql) matchesDatname(datname string) bool {
	if len(p.includeRegex) > 0 {
		var included bool
		for _, re := range p.includeRegex {
			if re.MatchString(datname) {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}
	for _, re := range p.excludeRegex {
		if re.MatchString(datname) {
			return false
		}
	}
	return true
}

func (p *Postgresql) gatherMetricsFromQuery(
	ctx context.Context,
	acc telegraf.Accumulator,
	service *postgresql.Service,
	j job,
	timestamp time.Time,
) error {
	if j.query.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(j.query.Timeout))
		defer cancel()
	}

	rows, err := service.DB.QueryContext(ctx, j.query.Sqlquery)
	if err != nil {
		return fmt.Errorf("query %q: %w", j.query.Measurement, err)
	}
	defer rows.Close()

	// grab the column information from the result
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	numeric := make(map[string]bool, len(columnTypes))
	for _, ct := range columnTypes {
		if ct.DatabaseTypeName() == "NUMERIC" {
			numeric[ct.Name()] = true
		}
	}

	for rows.Next() {
		if err := p.accRow(acc, rows, columns, numeric, j, timestamp); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (p *Postgresql) accRow(
	acc telegraf.Accumulator,
	row scanner,
	columns []string,
	numeric map[string]bool,
	j job,
	timestamp time.Time,
) error {
	q := j.query

	// this is where we'll store the column name with its *interface{}
	columnMap := make(map[string]*interface{})

	for _, column := range columns {
		columnMap[column] = new(interface{})
	}

	columnVars := make([]interface{}, 0, len(columnMap))
	// populate the array of interface{} with the pointers in the right order
	for i := 0; i < len(columnMap); i++ {
		columnVars = append(columnVars, columnMap[columns[i]])
	}

	// deconstruct array of variables and send to Scan
	if err := row.Scan(columnVars...); err != nil {
		return err
	}

	// extract the database name from the column map if available
	dbname := j.datname
	if c, ok := columnMap["datname"]; ok && *c != nil {
		if datname, ok := (*c).(string); ok {
			dbname = datname
		}
	}

	tags := map[string]string{
		"server": p.service.SanitizedAddress,
		"db":     dbname,
	}

	fields := make(map[string]interface{})
	for col, val := range columnMap {
		p.Log.Debugf("Column: %s = %T: %v\n", col, *val, *val)
		if q.ignoredColumns[col] || *val == nil {
			continue
		}

		if col == q.Timestamp {
			if v, ok := (*val).(time.Time); ok {
				timestamp = v
			}
			continue
		}

		if q.additionalTags[col] {
			v, err := internal.ToString(*val)
			if err != nil {
				p.Log.Debugf("Failed to add %q as additional tag: %v", col, err)
			} else {
				tags[col] = v
			}
			continue
		}

		if q.numericFloat && numeric[col] {
			v, err := toFloat(*val)
			if err != nil {
				p.Log.Debugf("Failed to convert numeric column %q: %v", col, err)
				continue
			}
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			fields[col] = v
			continue
		}

		switch v := (*val).(type) {
		case []byte:
			if q.stringTags {
				tags[col] = string(v)
			} else {
				fields[col] = string(v)
			}
		case string:
			if q.stringTags {
				tags[col] = v
			} else {
				fields[col] = v
			}
		default:
			fields[col] = *val
		}
	}
	acc.AddFields(q.Measurement, fields, tags, timestamp)
	return nil
}

// refreshDue reports whether the database list should be refreshed. The list
// is refreshed on the first collection after a refresh interval boundary on
// the wall clock, so with aligned collection intervals the refresh always
// happens on the same collection regardless of small timing jitter.
func refreshDue(updated, now time.Time, interval time.Duration) bool {
	if updated.IsZero() {
		return true
	}
	return !now.Before(updated.Truncate(interval).Add(interval))
}

func validateRole(role string) error {
	switch role {
	case "", "any", "primary", "replica":
		return nil
	default:
		return fmt.Errorf("invalid role %q, expected one of \"any\", \"primary\" or \"replica\"", role)
	}
}

func matchesVersion(q *query, dbVersion int) bool {
	return q.MinVersion <= dbVersion && (q.MaxVersion == 0 || q.MaxVersion > dbVersion)
}

func matchesRole(role string, inRecovery bool) bool {
	switch role {
	case "primary":
		return !inRecovery
	case "replica":
		return inRecovery
	default:
		return true
	}
}

func toFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, errors.New("unsupported type " + fmt.Sprintf("%T", value))
	}
}

func init() {
	inputs.Add("postgresql_multi", func() telegraf.Input {
		return &Postgresql{
			Config: postgresql.Config{
				MaxIdle: 1,
				MaxOpen: 1,
			},
			PreparedStatements: true,
			NumericAsFloat:     true,
			IgnoredColumns:     []string{"stats_reset"},
		}
	})
}
