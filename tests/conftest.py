"""Shared test fixtures for rextag."""

import pytest


@pytest.fixture
def sample_fiona_schema():
    """A typical Fiona schema as returned by collection.schema."""
    return {
        "geometry": "Polygon",
        "properties": {
            "OBJECTID": "int",
            "NAME": "str",
            "AREA_SQFT": "float",
            "CREATED_DATE": "date",
            "NOTES": "str",
        },
    }


@pytest.fixture
def sample_feature():
    """A single Fiona-style feature dict."""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-122.4194, 37.7749],
                [-122.4194, 37.7750],
                [-122.4193, 37.7750],
                [-122.4193, 37.7749],
                [-122.4194, 37.7749],
            ]],
        },
        "properties": {
            "OBJECTID": 1,
            "NAME": "Parcel A",
            "AREA_SQFT": 5000.5,
            "CREATED_DATE": "2024-01-15",
            "NOTES": None,
        },
    }


@pytest.fixture
def sample_feature_non_wgs84():
    """A feature in NAD83 / California zone 3 (EPSG:2227) -- units are feet."""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [6000000.0, 2100000.0],
        },
        "properties": {
            "OBJECTID": 1,
            "NAME": "Test Point",
        },
    }
