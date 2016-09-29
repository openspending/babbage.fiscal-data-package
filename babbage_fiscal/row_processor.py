import logging


class RowProcessor(object):

    def __init__(self, row_iterator, callback, schema, descriptor):
        self.row_iterator = row_iterator
        self.callback = callback
        self.schema = schema
        self.descriptor = descriptor
        self.field_order = list(map(lambda f: f['name'], schema['fields']))

        self.key_processors = {}

        self.setup_measure_factor_processing()
        # self.setup_partial_id_processing()

    def setup_measure_factor_processing(self):
        measures = self.descriptor.get('model',{}).get('measures',{})

        def multiplier(_factor):
            __factor = _factor*1

            def _ret(v, _):
                return v*__factor
            return _ret

        for _, measure in measures.items():
            factor = measure.get('factor',1)
            if factor != 1:
                self.key_processors.setdefault(measure.get('source'),[])\
                                   .append(multiplier(factor))

    # def setup_measure_factor_processing(self):
    #     partials =
    #     measures = self.descriptor.get('model',{}).get('measures',{})
    #     for _, measure in measures.items():
    #         factor = measure.get('factor',1)
    #         if factor != 1:
    #             self.key_processors.setdefault(measure.get('source'),[]) \
    #                 .append(lambda v, _: v*factor)

    def process_value(self, rec, key, value):
        for processor in self.key_processors.get(key, []):
            value = processor(value, rec)
        return value

    def iter(self):
        count = 0
        for rec in self.row_iterator:
            count += 1
            if count % 1000 == 1 and self.callback is not None:
                self.callback(count=count)
            rec = \
                (count,) + \
                tuple(self.process_value(rec, k, rec[k])
                      for k in self.field_order)
            yield rec

