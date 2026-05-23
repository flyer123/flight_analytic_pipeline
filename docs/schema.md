<h3>**Bronze (raw)**</h3>

**Keep as-is**
<h4>Flight (partitioned OPDI output)</h4>

    id                                str
    icao24                            str
    flt_id                            str
    callsign                          str (optional)

    dof                               datetime64[ns]
    first_seen                        datetime64[ns]
    last_seen                         datetime64[ns]

    adep                              str
    ades                              str
    estdepartureairport              str (optional)
    estarrivalairport                str (optional)

    registration                      str
    model                             str
    typecode                          str
    icao_aircraft_class               str
    icao_operator                     str

    version                           str
    unix_time                         int64 / str (source-dependent)


<h3>**Silver (cleaned)**</h3>

**Apply strict typing:**

<h4>Flight</h4>

    id                                str
    icao24                            str
    flt_id                            str

    first_seen_ts                    datetime64[ns]
    last_seen_ts                     datetime64[ns]

    adep                              str (normalized uppercase)
    ades                              str (normalized uppercase)

    ADEP                              str (derived uppercase alias)
    ADES                              str (derived uppercase alias)

    icao_operator                     str
    registration                      str (if present in source)
    model                             str (if present in source)
    typecode                          str (if present in source)


<h3>**Gold (analytics)**</h3>


<h4>Yearly Traffic (gold_yearly_flight_traffic)</h4>

    year                              int

    total_flights                     int

<h4>Monthly Traffic (gold_monthly_traffic)</h4>

    year                              int

    month                             int (1–12)

    month_start                       date (DATE_TRUNC('month', departure))

    total_flights                     int

    unique_aircraft                   int (COUNT DISTINCT icao24)

    avg_duration_hours                float (AVG flight duration in hours)

    flights_with_departure_airport   int (COUNT non-null departure_airport)

    flights_with_arrival_airport     int (COUNT non-null arrival_airport)

    departure_airport_coverage_pct   float (% non-null departure_airport)

    arrival_airport_coverage_pct     float (% non-null arrival_airport)

<h4>Average Flight Duration Trend (gold_avg_duration_trend)</h4>

    year                              int

    avg_duration_hours                float (AVG flight duration in hours)

<h4>Airport Completeness (gold_airport_completeness)</h4>

    year                              int

    total_flights                     int

    flights_with_departure_airport    int (COUNT non-null departure_airport)

    flights_with_arrival_airport      int (COUNT non-null arrival_airport)

    missing_departure_airport         int (total_flights - flights_with_departure_airport)

    missing_arrival_airport           int (total_flights - flights_with_arrival_airport)

    departure_airport_coverage_pct    float (% non-null departure_airport)

    arrival_airport_coverage_pct      float (% non-null arrival_airport)

<h4>Aircraft Trend (gold_aircraft_trend)</h4>

    year                              int

    unique_aircraft                   int (COUNT DISTINCT icao24)
