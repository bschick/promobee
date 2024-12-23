package promobee

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jdgeenen/egobee"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type thermostatMetrics struct {
	tempMetric         *prometheus.GaugeVec
	hvacModeMetric     *prometheus.GaugeVec
	holdTempMetric     *prometheus.GaugeVec
	hvacInOperation    *prometheus.GaugeVec
	humidityMetric     *prometheus.GaugeVec
	occupancyMetric    *prometheus.GaugeVec
	airQualityMetric   *prometheus.GaugeVec
	vocPpbMetric       *prometheus.GaugeVec
	co2PpmMetric       *prometheus.GaugeVec
	pressureMetric     *prometheus.GaugeVec
	vacationStateMetric *prometheus.GaugeVec
	coolingStateMetric *prometheus.GaugeVec
	heatingStateMetric *prometheus.GaugeVec
}

func newThermostatMetrics() *thermostatMetrics {
	return &thermostatMetrics{
		tempMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "temperature_fahrenheit",
				Help: "Temperature in Fahrenheit as reported by an Ecobee sensor.",
			},
			[]string{"location"}),
		holdTempMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hold_temperature_fahrenheit",
				Help: "Hold temperatures in Fahrenheit as reported by an Ecobee Thermostat",
			},
			[]string{"type"},
		),
		hvacModeMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hvac",
				Help: "HVAC mode as reported by an Ecobee thermostat.",
			},
			[]string{"mode"},
		),
		hvacInOperation: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "hvac_in_operation",
				Help: "Running HVAC equipment is emitted with a '1' metric",
			},
			[]string{"equipment"},
		),
		humidityMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "humidity",
				Help: "Humidity as reported by an Ecobee sensor.",
			},
			[]string{"location"}),
		airQualityMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "ecobee_air_quality",
				Help: "Air Quality as reported by an Ecobee sensor.",
			},
			[]string{"location"}),
		vocPpbMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "volatile_organic_compounds_ppb",
				Help: "Total Volatile Organic Compound concentration in PPB as reported by an Ecobee sensor.",
			},
			[]string{"location"}),
		co2PpmMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "carbon_dioxide_ppm",
				Help: "CO2 concentration in PPM as reported by an Ecobee sensor.",
			},
			[]string{"location"}),
		occupancyMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "occupancy",
				Help: "Occupancy as reported by an Ecobee sensor.",
			},
			[]string{"location"}),
		heatingStateMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "heating_state",
				Help: "Thermostat is actively heating.",
			},
			[]string{"location"}),
		vacationStateMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "vacation_state",
				Help: "Thermostat is in vacation movde.",
			},
			[]string{"location"}),
		coolingStateMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "cooling_state",
				Help: "Thermostat is actively cooling.",
			},
			[]string{"location"}),
		pressureMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "air_pressure_hectopascal",
				Help: "Outside air pressure.",
			},
			[]string{"location"}),
	}
}

var thermostatSelection = &egobee.Selection{
	SelectionType:   	egobee.SelectionTypeRegistered,
	IncludeDevice:   	true,
	IncludeEvents:   	true,
	IncludeRuntime:  	true,
	IncludeSensors:  	true,
	IncludeSettings:	true,
	IncludeWeather:		true,
	IncludeEquipmentStatus:	true,
}

// Accumulator of Ecobee information for reexport.
type Accumulator struct {
	client *egobee.Client
	done   chan<- bool

	mu          sync.RWMutex // protects following members
	thermostats map[string]*thermostatMetrics
}

func (a *Accumulator) metricsForThermostatIdentifier(identifier *string) *thermostatMetrics {
	a.mu.RLock()
	t, ok := a.thermostats[*identifier]
	a.mu.RUnlock()

	if !ok {
		t = newThermostatMetrics()
		a.mu.Lock()
		a.thermostats[*identifier] = t
		a.mu.Unlock()
	}

	return t
}

func (a *Accumulator) poll() error {
	thermostats, err := a.client.Thermostats(thermostatSelection)
	if err != nil {
		return err // This error is unrecoverable.
	}
	if len(thermostats) < 1 {
		log.Printf("Payload contained no thermostats.")
		// Not technically an error. Just inconvenient.
		return nil
	} else {
		log.Printf("Polling for %d thermostats.", len(thermostats))
	}
	for _, thermostat := range thermostats {
		if len(thermostat.RemoteSensors) < 1 {
			log.Printf("Thermostat has no sensors.")
			continue
		}
		m := a.metricsForThermostatIdentifier(&thermostat.Identifier)

		if thermostat.Runtime.ActualVoc != nil {
			voc := float64(*thermostat.Runtime.ActualVoc) / 4.09 // ug/m^3 to ppb
			m.vocPpbMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(voc)
			co2 := float64(*thermostat.Runtime.ActualCo2)
			m.co2PpmMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(co2)
			airQuality := float64(*thermostat.Runtime.ActualAQScore)
			m.airQualityMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(airQuality)
		}

		weatherStation := thermostat.Weather.WeatherStation
		temperature := float64(thermostat.Weather.Forecasts[0].Temperature) / 10
		m.tempMetric.With(prometheus.Labels{"location": weatherStation}).Set(temperature)
		pressure := float64(thermostat.Weather.Forecasts[0].Pressure)
		m.pressureMetric.With(prometheus.Labels{"location": weatherStation}).Set(pressure)
		humidity := float64(thermostat.Weather.Forecasts[0].RelativeHumidity)
		m.humidityMetric.With(prometheus.Labels{"location": weatherStation}).Set(humidity)

		m.heatingStateMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(0)
		m.coolingStateMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(0)

		equipmentStatus := thermostat.EquipmentStatus
		if strings.Contains(strings.ToLower(equipmentStatus), "heat") {
			m.heatingStateMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(1)
		} else if strings.Contains(strings.ToLower(equipmentStatus), "cool") {
			m.coolingStateMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(1)
		}

		m.holdTempMetric.Reset()

		m.vacationStateMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(0)
		if thermostat.Settings.HVACMode != "off" {
			for _, event := range thermostat.Events {
				if event.Running && event.Type == "hold" {
					if !event.IsCoolOff && thermostat.Settings.HVACMode != "heat" {
						m.holdTempMetric.WithLabelValues("cool").Set(float64(event.CoolHoldTemp) / 10)
					}
					if !event.IsHeatOff && thermostat.Settings.HVACMode != "cool" {
						m.holdTempMetric.WithLabelValues("heat").Set(float64(event.HeatHoldTemp) / 10)
					}
				} else if event.Running && event.Type == "vacation" {
					m.vacationStateMetric.With(prometheus.Labels{"location": thermostat.Name}).Set(1)
				}
			}
		}

		m.hvacModeMetric.Reset()
		m.hvacModeMetric.WithLabelValues(thermostat.Settings.HVACMode).Set(1)

		for _, sensor := range thermostat.RemoteSensors {
			h, err := sensor.Humidity()
			// Only handle the successful case; if the sensor doesn't have humidity, that isn't fatal
			if err == nil {
				m.humidityMetric.With(prometheus.Labels{"location": sensor.Name}).Set(float64(h))
			}

			o, err := sensor.Occupancy()
			// Only handle the successful case; if the sensor doesn't have occupancy, that isn't fatal
			if err == nil {
				v := 0.0
				if o {
					v = 1.0
				}
				m.occupancyMetric.With(prometheus.Labels{"location": sensor.Name}).Set(v)
			}

			t, err := sensor.Temperature()
			if err != nil {
				// We may still be able to get useful information from the payload,
				// so skip this error.
				log.Printf("Error getting temperature from %q: %v", sensor.Name, err)
				continue
			}
			m.tempMetric.With(prometheus.Labels{"location": sensor.Name}).Set(t)
		}
	}

	statSummary, err := a.client.ThermostatSummary()
	if err != nil {
		return err
	}

	for _, status := range statSummary.StatusList {
		d := strings.Split(status, ":")
		if len(d) != 2 {
			log.Printf("Thermostat status '%s' did not have two fields", status)
			continue
		}
		m := a.metricsForThermostatIdentifier(&d[0])
		m.hvacInOperation.Reset()
		if d[1] != "" {
			for _, unit := range strings.Split(d[1], ",") {
				m.hvacInOperation.WithLabelValues(unit).Set(1)
			}
		}
	}

	return nil
}

// ServeThermostatsList is a http.HandlerFunc which serves the list of known
// Thermostat identifers.
func (a *Accumulator) ServeThermostatsList(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	ids := make([]string, 0)
	a.mu.RLock()
	for id := range a.thermostats {
		ids = append(ids, id)
	}
	a.mu.RUnlock()

	sort.Strings(ids) // consistency!
	for _, id := range ids {
		fmt.Fprintf(w, "%v\n", id)
	}
}

// ServeThermostat is a http.HandlerFunc which serves the
func (a *Accumulator) ServeThermostat(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id")
	if id == "" {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not Found")
		return
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	t, ok := a.thermostats[id]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not Found")
		return
	}

	registry := prometheus.NewRegistry()
	metrics := []*prometheus.GaugeVec{t.tempMetric, t.occupancyMetric, t.humidityMetric, t.airQualityMetric, t.co2PpmMetric, t.vocPpbMetric, t.pressureMetric, t.holdTempMetric, t.hvacInOperation, t.hvacModeMetric, t.heatingStateMetric, t.coolingStateMetric, t.vacationStateMetric}
	for _, m := range metrics {
		if err := registry.Register(m); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Internal Server Error")
			return
		}
	}

	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, req)
}

// Stop polling the Ecobee API.
func (a *Accumulator) Stop() {
	a.done <- true
}

// The Ecobee API docs recommend polling no more frequently than 3 minutes.
var defaultPollInterval = time.Minute * 3

// Opts for the Accumulator.
type Opts struct {
	PollInterval time.Duration
}

func (o *Opts) pollInterval() time.Duration {
	if o == nil || o.PollInterval == 0 {
		return defaultPollInterval
	}
	return o.PollInterval
}

// New Accumulator.
func New(c *egobee.Client, o *Opts) *Accumulator {
	done := make(chan bool)
	a := &Accumulator{
		client:      c,
		done:        done,
		thermostats: make(map[string]*thermostatMetrics),
	}

	go func(a *Accumulator, done <-chan bool) {
		ticker := time.NewTicker(o.pollInterval())
		if err := a.poll(); err != nil {
			log.Printf("error polling: %v", err)
		}
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := a.poll(); err != nil {
					log.Printf("error polling: %v", err)
				}
			}
		}
	}(a, done)

	return a
}
