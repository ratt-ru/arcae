from numpy.testing import assert_array_equal

from arcae.lib.arrow_tables import Table, ms_descriptor


def test_descriptor_basic():
    assert isinstance(ms_descriptor("MAIN"), dict)
    assert isinstance(ms_descriptor("ANTENNA"), dict)
    assert isinstance(ms_descriptor("FEED"), dict)
    assert isinstance(ms_descriptor("SPECTRAL_WINDOW"), dict)


def test_ms_addrows(tmp_path_factory):
    ms = tmp_path_factory.mktemp("test") / "test.ms"
    with Table.ms_from_descriptor(str(ms)) as T:
        T.addrows(10)
        assert T.nrow() == 10
        AT = T.to_arrow()
        assert len(AT) == 10
        assert_array_equal(AT.column("TIME"), 0)
        assert_array_equal(AT.column("ANTENNA1"), 0)
        assert_array_equal(AT.column("ANTENNA2"), 0)


def test_ms_and_weather_subtable(tmp_path_factory):
    ms = tmp_path_factory.mktemp("test") / "test.ms"
    with Table.ms_from_descriptor(str(ms)):
        assert (ms / "table.dat").exists()
        assert not (ms / "WEATHER").exists()

    # Basic descriptor
    table_desc = ms_descriptor("WEATHER", complete=False)
    with Table.ms_from_descriptor(str(ms), "WEATHER", table_desc) as W:
        assert (ms / "WEATHER").exists()
        assert W.columns() == ["ANTENNA_ID", "INTERVAL", "TIME"]

    # Add a column to the basic descriptor
    table_desc = ms_descriptor("WEATHER", complete=False)
    table_desc["BLAH"] = table_desc["TIME"].copy()
    with Table.ms_from_descriptor(str(ms), "WEATHER", table_desc) as W:
        assert (ms / "WEATHER").exists()
        assert W.columns() == ["ANTENNA_ID", "BLAH", "INTERVAL", "TIME"]

    # Complete descriptor
    table_desc = ms_descriptor("WEATHER", complete=True)
    with Table.ms_from_descriptor(str(ms), "WEATHER", table_desc) as W:
        assert (ms / "WEATHER").exists()
        assert W.columns() == [
            "ANTENNA_ID",
            "DEW_POINT",
            "DEW_POINT_FLAG",
            "H2O",
            "H2O_FLAG",
            "INTERVAL",
            "IONOS_ELECTRON",
            "IONOS_ELECTRON_FLAG",
            "PRESSURE",
            "PRESSURE_FLAG",
            "REL_HUMIDITY",
            "REL_HUMIDITY_FLAG",
            "TEMPERATURE",
            "TEMPERATURE_FLAG",
            "TIME",
            "WIND_DIRECTION",
            "WIND_DIRECTION_FLAG",
            "WIND_SPEED",
            "WIND_SPEED_FLAG",
        ]


def test_weather_subtable_descriptor():
    # Test required and complete descriptor for the WEATHER subtable
    assert ms_descriptor("WEATHER", complete=False) == {
        "ANTENNA_ID": {
            "comment": "Antenna number",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "int",
        },
        "INTERVAL": {
            "comment": "Interval over which data is relevant",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["s"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "double",
        },
        "TIME": {
            "comment": "An MEpoch specifying the midpoint of the time forwhich "
            "data is relevant",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {
                "MEASINFO": {"Ref": "UTC", "type": "epoch"},
                "QuantumUnits": ["s"],
            },
            "maxlen": 0,
            "option": 0,
            "valueType": "double",
        },
        "_define_hypercolumn_": {},
        "_keywords_": {},
        "_private_keywords_": {},
    }

    assert ms_descriptor("WEATHER", complete=True) == {
        "ANTENNA_ID": {
            "comment": "Antenna number",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "int",
        },
        "DEW_POINT": {
            "comment": "Dew point",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["K"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "DEW_POINT_FLAG": {
            "comment": "Flag for dew point",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "H2O": {
            "comment": "Average column density of water-vapor",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["m-2"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "H2O_FLAG": {
            "comment": "Flag for average column density of water-vapor",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "INTERVAL": {
            "comment": "Interval over which data is relevant",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["s"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "double",
        },
        "IONOS_ELECTRON": {
            "comment": "Average column density of electrons",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["m-2"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "IONOS_ELECTRON_FLAG": {
            "comment": "Flag for average column density of electrons",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "PRESSURE": {
            "comment": "Ambient atmospheric pressure",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["hPa"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "PRESSURE_FLAG": {
            "comment": "Flag for ambient atmospheric pressure",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "REL_HUMIDITY": {
            "comment": "Ambient relative humidity",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["%"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "REL_HUMIDITY_FLAG": {
            "comment": "Flag for ambient relative humidity",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "TEMPERATURE": {
            "comment": "Ambient Air Temperature for an antenna",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["K"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "TEMPERATURE_FLAG": {
            "comment": "Flag for ambient Air Temperature for an antenna",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "TIME": {
            "comment": "An MEpoch specifying the midpoint of the time forwhich "
            "data is relevant",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {
                "MEASINFO": {"Ref": "UTC", "type": "epoch"},
                "QuantumUnits": ["s"],
            },
            "maxlen": 0,
            "option": 0,
            "valueType": "double",
        },
        "WIND_DIRECTION": {
            "comment": "Average wind direction",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["rad"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "WIND_DIRECTION_FLAG": {
            "comment": "Flag for wind direction",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "WIND_SPEED": {
            "comment": "Average wind speed",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {"QuantumUnits": ["m/s"]},
            "maxlen": 0,
            "option": 0,
            "valueType": "float",
        },
        "WIND_SPEED_FLAG": {
            "comment": "Flag for wind speed",
            "dataManagerGroup": "StandardStMan",
            "dataManagerType": "StandardStMan",
            "keywords": {},
            "maxlen": 0,
            "option": 0,
            "valueType": "boolean",
        },
        "_define_hypercolumn_": {},
        "_keywords_": {},
        "_private_keywords_": {},
    }
