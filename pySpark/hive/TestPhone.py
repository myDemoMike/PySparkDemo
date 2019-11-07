import re

phone = "13963422012"
phone_pat = re.compile(
    "[1][3,5,8,9][0-9]{9}$|^[9][2,8][0-9]{9}$|^14[5|6|7|8|9][0-9]{8}$|^16[1|2|4|5|6|7][0-9]{8}$|^17[0-8][0-9]{8}")
res = phone_pat.search(phone)
if res is None:
    print("0")
else:
    print("1")

