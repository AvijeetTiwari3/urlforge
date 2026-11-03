package model

import (
	"time"
)

// ClickEvent represents an asynchronous message published to Kafka on redirect
type ClickEvent struct {
	ShortCode string    `json:"short_code"`
	ClickedAt time.Time `json:"clicked_at"`
	IPHash    string    `json:"ip_hash"`
	UserAgent string    `json:"user_agent"`
	Referrer  string    `json:"referrer"`
	Country   string    `json:"country"`
	Device    string    `json:"device"`
}

// CountryMetric represents aggregated clicks per geographic origin
type CountryMetric struct {
	Country string `json:"country"`
	Clicks  int64  `json:"clicks"`
}

// HourlyMetric represents clicks aggregated per time bucket
type HourlyMetric struct {
	Hour   time.Time `json:"hour"`
	Clicks int64     `json:"clicks"`
}

// AnalyticsResponse represents the aggregated analytics payload
type AnalyticsResponse struct {
	ShortCode       string            `json:"short_code"`
	TotalClicks     int64             `json:"total_clicks"`
	UniqueClicks    int64             `json:"unique_clicks"`
	TopCountries    []CountryMetric   `json:"top_countries"`
	ClicksOverTime  []HourlyMetric    `json:"clicks_over_time"`
	DeviceBreakdown map[string]int64  `json:"device_breakdown"`
}
