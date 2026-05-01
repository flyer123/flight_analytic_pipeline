<h3>**Bronze (raw)**</h3>

**Keep as-is**
<h4>Flight</h4>

    id                                str
    icao24                            str
    flt_id                            str
    dof                               str
    adep                              str
    ades                              str
    adep_p                            str
    ades_p                            str
    registration                      str
    model                             str
    typecode                          str
    icao_aircraft_class               str
    icao_operator                     str
    first_seen                        str
    last_seen                         str
    version                           str
    unix_time                         str

<h4>Airports</h4>

    id                       str 
    ident                    str
    type                     str
    name                     str
    latitude_deg             str
    longitude_deg            str
    elevation_ft             str
    continent                str
    iso_country              str
    iso_region               str
    municipality             str
    scheduled_service        str
    icao_code                str
    iata_code                str
    gps_code                 str
    local_code               str
    home_link                str
    wikipedia_link           str
    keywords                 str

<h3>**Silver (cleaned)**</h3>

**Apply strict typing:**

<h4>Flight</h4>

    id                                int
    icao24                            str
    flt_id                            str
    dof                    datetime64[us]
    adep                              str
    ades                              str
    adep_p                            str
    ades_p                            str
    registration                      str
    model                             str
    typecode                          str
    icao_aircraft_class               str
    icao_operator                     str
    first_seen             datetime64[us]
    last_seen              datetime64[us]
    version                           str
    unix_time                        int64
    flight_duration_sec(calculated)  int64
    flight_duration_min(calculated)  int64
    flight_duration_hour(calculated) int64     
    registration                      str
    model                             str
    typecode                          str
<h4>Airports</h4>

    id                     int64
    ident                    str
    type                     str
    name                     str
    latitude_deg         float64
    longitude_deg        float64
    elevation_ft         float64
    continent                str
    iso_country              str
    iso_region               str
    municipality             str
    scheduled_service        str
    icao_code                str
    iata_code                str
    gps_code                 str
    local_code               str
    home_link                str
    wikipedia_link           str
    keywords                 str

<h3>**Gold (analytics)**</h3>

**Star schema:**

<h4>fact_flights</h4>

    flight_id STRING,
    departure_airport_id ID,
    arrival_airport_id ID,
    airline_id INT,
    aircraft_id INT,
    duration_minutes INT,
    date_id INT

<h4>dim_airport</h4>

    airport_id INT,
    airport_code STRING,
    city STRING,
    country STRING

<h4>dim_dates</h4>

    date_id int,
    date date

<h4>dim_aircraft</h4>

    aircraft_id INT,
    registration STR,
    model STR,
    typecode STR,
    icao_aircraft_class STR

<h4>dim_airlines</h4>
    
    airline_id INT,
    ariline-code STR

    


