import logging

import time


class RowProcessor(object):

    def __init__(self, row_iterator, callback, schema, descriptor):
        self.row_iterator = row_iterator
        self.callback = callback
        self.schema = schema
        self.descriptor = descriptor
        self.field_order = list(map(lambda f: f['name'], schema['fields']))
        self.fields = dict(zip(self.field_order, self.schema['fields']))

        self.key_processors = {}

        self.setup_measure_factor_processing()
        self.setup_partial_id_processing()

    def add_field_processor(self, field_name, processor):
        self.key_processors.setdefault(field_name, []) \
            .append(processor)

    def setup_measure_factor_processing(self):
        measures = self.descriptor.get('model',{}).get('measures', {})

        def multiplier(_factor):
            __factor = _factor*1

            def _ret(v, _):
                return v*__factor
            return _ret

        for _, measure in measures.items():
            factor = measure.get('factor',1)
            if factor != 1:
                self.add_field_processor(measure['source'],
                                         multiplier(factor))

    def setup_partial_id_processing(self):
        partials = {}
        dimensions = self.descriptor['model']['dimensions']
        for dimension_name, dimension in dimensions.items():
            attributes = dimension['attributes']
            primary_keys = dimension.get('primaryKey', [])
            if type(primary_keys) is not list:
                primary_keys = [primary_keys]
            for key_attr_name in primary_keys:
                key_attr = attributes[key_attr_name]
                key_field = self.fields[key_attr['source']]
                key_field_name = key_field['name']
                os_type = key_field.get('osType')
                if os_type is not None:
                    if os_type.endswith(':code:full') or os_type.endswith(':code'):
                        partials[key_field_name] = [key_field_name]
                    elif os_type.endswith(':code:part'):
                        parent_attr_name = key_attr['parent']
                        parent_attr = attributes[parent_attr_name]
                        parent_field = parent_attr['source']
                        parent_partials = partials[parent_field]
                        partials[key_field_name] = parent_partials + [key_field_name]

        def combiner(_partials):
            __partials = _partials[:]

            def ret(_, rec):
                if len(rec[__partials[-1]].strip()) == 0:
                    return ''
                return ' - '.join(rec[k] for k in __partials)

            return ret

        for field_name, combinations in partials.items():
            if len(combinations) > 1:
                self.add_field_processor(field_name,
                                         combiner(combinations))

    def process_value(self, rec, key, value):
        for processor in self.key_processors.get(key, []):
            value = processor(value, rec)
        return value

    def iter(self):
        count = 0
        sent = 0
        for rec in self.row_iterator:
            count += 1
            if self.callback is not None:
                t = time.time()
                if t - sent > 1:
                    self.callback(count=count)
                    sent = t
            rec = \
                (count,) + \
                tuple(self.process_value(rec, k, rec[k])
                      for k in self.field_order)
            yield rec

