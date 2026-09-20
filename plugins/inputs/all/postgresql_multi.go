//go:build !custom || inputs || inputs.postgresql_multi

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/postgresql_multi" // register plugin
