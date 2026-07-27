from Crypto.Cipher import AES
import base64
import datetime
from rubymarshal.classes import UsrMarshal, ClassRegistry
import rubymarshal.reader
import sys


# Borg production values
BORG_IV = b"2e73243957efc302"
BORG_KEY = base64.b64decode("ZmZiYjQxNjhhOGE4MTFhODZhMzY4NzQ2MDhkMTk4OGQ=")


class RubyDate(UsrMarshal):
    ruby_class_name = "Date"

    def marshal_load(self, attr_list):
        julian_day = attr_list[1]
        self.set_attributes({'date': datetime.date.fromordinal(julian_day - 1721425)})


_registry = ClassRegistry()
_registry.register(RubyDate)


def decrypt(ciphertext):
    cipher = AES.new(BORG_KEY, AES.MODE_CBC, iv=BORG_IV)
    padded_bytes = cipher.decrypt(base64.b64decode(ciphertext))

    return padded_bytes.rstrip(bytes([padded_bytes[-1]]))


def decrypt_string(ciphertext):
    return decrypt(ciphertext).decode('utf-8')


def decrypt_date(ciphertext):
    marshalled_date = decrypt(ciphertext)
    ruby_date = rubymarshal.reader.loads(marshalled_date, registry=_registry)
    return ruby_date.attributes['date']
