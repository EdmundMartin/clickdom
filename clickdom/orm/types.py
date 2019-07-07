from clickdom.orm.abstract import ValidatedField


class UInt8(ValidatedField):
    click_house_types = ['UInt8', ]

    def _validate_one(self, value):
        if not isinstance(value, int):
            raise ValueError('Invalid UInt8')
        return value

    def validate(self, instance, value):
        return self._validate_one(value)