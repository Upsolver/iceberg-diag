import pytest

from icebergdiag.utils import OrderedEnum


@pytest.fixture(scope="module")
def test_enum():
    class TestEnum(OrderedEnum):
        BETA = "beta"
        ALPHA = "alpha"
        GAMMA = "gamma"

    return TestEnum


def test_enum_sorting(test_enum):
    expected_order = [test_enum.BETA, test_enum.ALPHA, test_enum.GAMMA]
    sorted_enum = sorted([test_enum.GAMMA, test_enum.BETA, test_enum.ALPHA])
    assert sorted_enum == expected_order


def test_enum_comparison(test_enum):
    assert test_enum.BETA < test_enum.ALPHA
    assert test_enum.ALPHA < test_enum.GAMMA
    assert test_enum.GAMMA > test_enum.BETA
    assert test_enum.BETA != test_enum.GAMMA


def test_regular_enum_usage(test_enum):
    assert test_enum.BETA.value == "beta"
    assert test_enum.ALPHA.value == "alpha"
    assert test_enum.GAMMA.value == "gamma"


def test_enum_iteration(test_enum):
    expected_order = ["beta", "alpha", "gamma"]
    actual_order = [member.value for member in test_enum]
    assert actual_order == expected_order
