from pydragon import DictStream


def test_dict_main():
    random_dict = {k: k for k in range(10)}
    s1 = DictStream(random_dict) \
        .map_keys(lambda k: chr(0x41 + k)) \
        .build_dict()
    assert s1 == {chr(0x41 + k): k for k in range(10)}

