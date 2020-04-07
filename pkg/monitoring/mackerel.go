package monitoring

import (
	"fmt"

	"github.com/mackerelio/mackerel-client-go"
)

func PostServiceMetricValues(apikey, service string, values []*mackerel.MetricValue) error {
	client := mackerel.NewClient(apikey)
	if err := client.PostServiceMetricValues(service, values); err != nil {
		return fmt.Errorf("post service metirc values: %v\n", err)
	}

	return nil
}
