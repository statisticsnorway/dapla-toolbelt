import dapla


def test_version_attribute() -> None:
    assert dapla.__version__ != "0.0.0"
