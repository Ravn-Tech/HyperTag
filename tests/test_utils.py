from hypertag import utils


def test_is_int():
    assert utils.is_int("42") == True
    assert utils.is_int("3.4") == False
    assert utils.is_int("ABC") == False
    assert utils.is_int(3.141) == False
