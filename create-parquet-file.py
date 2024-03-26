import sys
import leb128

import thrift.transport.TTransport
import thrift.protocol.TCompactProtocol

def thrift_to_bytes(thrift_object):
	transport = thrift.transport.TTransport.TMemoryBuffer()
	protocol = thrift.protocol.TCompactProtocol.TCompactProtocol(transport)
	thrift_object.write(protocol)
	return transport.getvalue()

# parquet-specific
sys.path.append('gen-py')
from parquet.ttypes import *

schema = [
	SchemaElement(name = "r", num_children = 1, repetition_type = FieldRepetitionType.REQUIRED), 
	SchemaElement(type = Type.INT64, name = "b", num_children = 0, repetition_type = FieldRepetitionType.REQUIRED)
]
schema[0].validate()
schema[1].validate()

out = open('42.parquet', 'wb')
out.write('PAR1'.encode())

col_start = out.tell()
dictionary_offset = out.tell()

# we write a single-value dictionary with int64-max in it

page_header_1 = PageHeader(type = PageType.DICTIONARY_PAGE, uncompressed_page_size = 8, compressed_page_size = 8,  dictionary_page_header = DictionaryPageHeader(num_values = 1, encoding = Encoding.PLAIN))
page_header_1.validate()
page_header_1.dictionary_page_header.validate()

page_header_1_bytes = thrift_to_bytes(page_header_1)
out.write(page_header_1_bytes)
out.write((9_223_372_036_854_775_807).to_bytes(8, byteorder='little'))

data_offset = out.tell()

# and now we refer to this single entry a gazillion times
page_repeat = 1000
row_group_repeat = 290
page_values = 2_147_483_647 # max int, we can't fit more in a page
num_values = page_values * page_repeat * row_group_repeat

data_page_content = bytearray([1]) + leb128.u.encode(page_values << 1) + bytearray([0])

page_header_2 = PageHeader(type = PageType.DATA_PAGE, uncompressed_page_size = len(data_page_content), compressed_page_size = len(data_page_content),  data_page_header = DataPageHeader(num_values = page_values, encoding = Encoding.RLE_DICTIONARY, definition_level_encoding = Encoding.PLAIN, repetition_level_encoding = Encoding.PLAIN))
page_header_2.data_page_header.validate()
page_header_2.validate()

page_header_2_bytes = thrift_to_bytes(page_header_2)

page_bytes = page_header_2_bytes + data_page_content


for i in range(page_repeat):
	out.write(page_bytes)

column_bytes = out.tell() - col_start

meta_data = ColumnMetaData(type = Type.INT64, encodings = [Encoding.RLE_DICTIONARY], path_in_schema=["b"], codec = CompressionCodec.UNCOMPRESSED, num_values = num_values, total_uncompressed_size = column_bytes, total_compressed_size = column_bytes, data_page_offset = data_offset, dictionary_page_offset = dictionary_offset)
meta_data.validate()
column = ColumnChunk(file_offset = data_offset, meta_data = meta_data)
column.validate()

row_group = RowGroup(num_rows = num_values, total_byte_size = column_bytes, columns = [column])

row_group.validate()

file_meta_data = FileMetaData(version = 1, num_rows = num_values, schema=schema, row_groups = [row_group] * row_group_repeat)
file_meta_data.validate()

footer_bytes = thrift_to_bytes(file_meta_data)
out.write(footer_bytes)
out.write(len(footer_bytes).to_bytes(4, byteorder='little'))
out.write('PAR1'.encode())