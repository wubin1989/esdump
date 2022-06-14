package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wubin1989/esdump/v2/core"
	"time"
)

const version = "v1.0.0"

var (
	input      string
	output     string
	dumpType   string
	dateField  string
	startDate  string
	endDate    string
	step       time.Duration
	scrollSize int
	descending bool
	zone       string
	includes   string
	excludes   string
)

// rootCmd is the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Version: version,
	Use:     "esdump",
	Short:   "migrate index from one elasticsearch to another",
	Long:    ``,
	Run: func(cmd *cobra.Command, args []string) {
		dumper := core.NewDumper(core.Config{
			Input:      input,
			Output:     output,
			DumpType:   dumpType,
			DateField:  dateField,
			StartDate:  startDate,
			EndDate:    endDate,
			Step:       step,
			ScrollSize: scrollSize,
			Descending: descending,
			Zone:       zone,
			Includes:   includes,
			Excludes:   excludes,
		})
		dumper.Dump()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	rootCmd.Flags().StringVarP(&input, "input", "i", "", "source elasticsearch connection url")
	rootCmd.Flags().StringVarP(&output, "output", "o", "", `target elasticsearch connection url`)
	rootCmd.Flags().StringVarP(&dumpType, "type", "t", "", `migration type, such as "mapping", "data", empty means both`)
	rootCmd.Flags().StringVarP(&dateField, "date", "d", "", `date field of docs`)
	rootCmd.Flags().StringVarP(&startDate, "start", "s", "", `start date, use time.Local as time zone, you may need to set TZ environment variable ahead`)
	rootCmd.Flags().StringVarP(&endDate, "end", "e", "", `end date, use time.Local as time zone, you may need to set TZ environment variable ahead`)
	rootCmd.Flags().StringVarP(&zone, "zone", "z", "UTC", `time zone of the date type field specified by date flag`)
	rootCmd.Flags().StringVar(&includes, "includes", "", `includes fields, multiple fields are separated by comma`)
	rootCmd.Flags().StringVar(&excludes, "excludes", "", `excludes fields, multiple fields are separated by comma`)
	rootCmd.Flags().BoolVar(&descending, "desc", false, `ascending or descending order by the date type field specified by date flag`)
	rootCmd.Flags().DurationVar(&step, "step", 24*time.Hour, `step duration`)
	rootCmd.Flags().IntVarP(&scrollSize, "limit", "l", 1000, `limit for one scroll, it takes effect on the dumping speed`)
	rootCmd.MarkFlagRequired("input")
	rootCmd.MarkFlagRequired("output")
	if dumpType != "mapping" {
		rootCmd.MarkFlagRequired("date")
	}
}
