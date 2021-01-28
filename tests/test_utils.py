from hypertag import utils


def test_is_int():
    assert utils.is_int("42") is True
    assert utils.is_int("3.4") is False
    assert utils.is_int("ABC") is False
    assert utils.is_int(3.141) is False
